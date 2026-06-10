'use strict';

var net                    = require('net');
var NoKafkaConnectionError = require('./errors').NoKafkaConnectionError;
var tls                    = require('tls');

function Connection(options) {
    var defaults = {
        port: 9092,
        host: '127.0.0.1',
        connectionTimeout: 3000,
        socketTimeout: 0,
        requestTimeout: 300000, // per-request cap so a silent broker can't hang callers forever
        initialBufferSize: 256 * 1024,
        maxFrameSize: 100 * 1024 * 1024, // matches broker default socket.request.max.bytes
        ssl: {}
    };
    var opts = options || {}, optKeys = Object.keys(opts), k, ki;
    for (ki = 0; ki < optKeys.length; ki++) {
        k = optKeys[ki];
        if (opts[k] !== undefined) {
            defaults[k] = opts[k];
        }
    }
    this.options = defaults;

    // internal state
    this.connected = false;
    this.closed = false; // raised if close() was called
    this.buffer = Buffer.allocUnsafe(this.options.initialBufferSize);
    this.offset = 0;

    this.queue = {};

    // discovered API versions from broker (null = not yet discovered)
    this.apiVersions = null;
}

module.exports = Connection;

Connection.prototype.equal = function (host, port) {
    return this.options.host === host && this.options.port === port;
};

Connection.prototype.server = function () {
    return this.options.host + ':' + this.options.port;
};

Connection.prototype.connect = function () {
    var self = this;

    if (self.connected) {
        return Promise.resolve();
    }

    if (self.connecting) {
        return self.connecting;
    }

    self.connecting = new Promise(function (resolve, reject) {
        var connectTimer, onConnect, _err, useTls;

        connectTimer = setTimeout(function () {
            if (self.socket) {
                self.socket.destroy();
            }
            reject(new NoKafkaConnectionError(self.server(), 'Connection timeout'));
        }, self.options.connectionTimeout);

        onConnect = function () {
            clearTimeout(connectTimer);
            if (self.closed) {
                // close() ran while the TCP handshake was in flight
                self.socket.destroy();
                reject(new NoKafkaConnectionError(self.server(), 'Connection was closed during connect.'));
                return;
            }
            self.connected = true;
            resolve();
        };

        if (self.socket) {
            self.socket.destroy();
        }

        // If we have aborted/stopped during startup, don't progress.
        if (self.closed) {
            clearTimeout(connectTimer);
            reject(new NoKafkaConnectionError(self.server(), 'Connection was aborted before connection was established.'));
            return;
        }

        // TLS is used when explicitly requested (ssl: true or ssl.enabled) or when
        // TLS material (cert+key or ca) is provided. Explicit requests must never
        // fall back to plaintext.
        useTls = self.options.ssl === true
            || (self.options.ssl && (self.options.ssl.enabled === true
                || (self.options.ssl.cert && self.options.ssl.key)
                || self.options.ssl.ca));

        if (useTls) {
            self.socket = tls.connect(self.options.port, self.options.host,
                self.options.ssl === true ? {} : self.options.ssl, onConnect);
        } else {
            self.socket = net.connect(self.options.port, self.options.host, onConnect);
        }

        self.socket.setNoDelay(true);
        self.socket.setKeepAlive(true, 60000);
        self.socket.setTimeout(self.options.socketTimeout);

        self.socket.on('timeout', function () {
            self._disconnect(new NoKafkaConnectionError(self.server(), 'Socket timeout'), true);
        });
        self.socket.on('end', function () {
            self._disconnect(new NoKafkaConnectionError(self.server(), 'Kafka server has closed connection'));
        });
        self.socket.on('error', function (err) {
            clearTimeout(connectTimer);
            _err = new NoKafkaConnectionError(self.server(), err.toString());
            reject(_err);
            self._disconnect(_err, true);
        });
        self.socket.on('data', self._receive.bind(self));
    })
    .finally(function () {
        self.connecting = false;
    });

    return self.connecting;
};

// Private disconnect method, this is what the 'end' and 'error'
// events call directly to make sure internal state is maintained
Connection.prototype._disconnect = function (err, force) {
    var keys, i, entry;

    // always settle pending requests, even when already disconnected — a send()
    // racing a disconnect can strand an entry in a fresh queue otherwise
    keys = Object.keys(this.queue);
    for (i = 0; i < keys.length; i++) {
        entry = this.queue[keys[i]];
        clearTimeout(entry.timer);
        entry.reject(err);
    }
    this.queue = {};

    if (!this.connected) {
        return;
    }

    if (force) {
        this.socket.destroy();
    } else {
        this.socket.end();
    }
    this.connected = false;
    this.offset = 0;
};

Connection.prototype._growBuffer = function (newLength) {
    var _b = Buffer.allocUnsafe(newLength);
    this.buffer.copy(_b, 0, 0, this.offset);
    this.buffer = _b;
};

Connection.prototype.close = function () {
    var err = new NoKafkaConnectionError(this.server(), 'Connection closed');
    err._kafka_connection_closed = true;
    this.closed = true;
    this._disconnect(err);
    // a connect may still be in flight — make sure its socket doesn't survive
    if (this.socket && !this.connected) {
        this.socket.destroy();
    }
};

Connection.prototype.cancelPending = function (correlationId) {
    if (this.queue[correlationId]) {
        clearTimeout(this.queue[correlationId].timer);
        delete this.queue[correlationId];
    }
};

/**
 * Send a request to Kafka
 *
 * @param  {Buffer} data request message
 * @param  {Boolean} noresponse if the server wont send any response to this request
 * @return {Promise}      Promise resolved with a Kafka response message
 */
Connection.prototype.send = function (correlationId, data, noresponse) {
    var self = this, buffer = Buffer.allocUnsafe(4 + data.length);

    buffer.writeInt32BE(data.length, 0);
    data.copy(buffer, 4);

    function _send() {
        return new Promise(function (resolve, reject) {
            var entry = {
                resolve: resolve,
                reject: reject,
                timer: null
            };

            if (self.options.requestTimeout > 0 && noresponse !== true) {
                entry.timer = setTimeout(function () {
                    delete self.queue[correlationId];
                    reject(new NoKafkaConnectionError(self.server(),
                        'Request timed out after ' + self.options.requestTimeout + 'ms'));
                }, self.options.requestTimeout);
            }

            self.queue[correlationId] = entry;

            try {
                self.socket.write(buffer);
            } catch (e) {
                clearTimeout(entry.timer);
                delete self.queue[correlationId];
                throw e; // rejects this promise via the executor
            }

            if (noresponse === true) {
                entry.resolve();
                delete self.queue[correlationId];
            }
        });
    }

    if (!self.connected) {
        return self.connect().then(function () {
            return _send();
        });
    }

    return _send();
};

Connection.prototype._receive = function (data) {
    var length, correlationId, dataOffset, remaining;

    if (!this.connected) {
        return;
    }

    if (this.offset) {
        if (this.buffer.length < data.length + this.offset) {
            this._growBuffer(data.length + this.offset);
        }
        data.copy(this.buffer, this.offset);
        this.offset += data.length;
        data = this.buffer.slice(0, this.offset);
    }

    dataOffset = 0;

    while (dataOffset < data.length) {
        if (data.length - dataOffset < 4) {
            break;
        }

        length = data.readInt32BE(dataOffset);

        // Every response frame contains at least a 4-byte correlationId. A negative
        // or undersized length desyncs framing (or loops forever); an oversized one
        // is a memory-exhaustion vector. Both mean the stream is unusable.
        if (length < 4 || length > this.options.maxFrameSize) {
            this._disconnect(new NoKafkaConnectionError(this.server(),
                'Invalid response frame length ' + length + ' (max allowed ' + this.options.maxFrameSize + ')'), true);
            return;
        }

        if (data.length - dataOffset < 4 + length) {
            break;
        }

        correlationId = data.readInt32BE(dataOffset + 4);

        if (this.queue.hasOwnProperty(correlationId)) {
            clearTimeout(this.queue[correlationId].timer);
            this.queue[correlationId].resolve(Buffer.from(data.slice(dataOffset + 4, dataOffset + 4 + length)));
            delete this.queue[correlationId];
        }

        dataOffset += 4 + length;
    }

    // buffer any remaining partial frame
    if (dataOffset < data.length) {
        remaining = data.length - dataOffset;
        if (this.offset === 0 && dataOffset === 0) {
            // first partial frame — copy into receive buffer
            if (this.buffer.length < data.length) {
                this._growBuffer(data.length);
            }
            data.copy(this.buffer);
            this.offset = data.length;
        } else if (dataOffset > 0) {
            // processed some frames, leftover is a partial frame
            if (this.buffer.length < remaining) {
                this._growBuffer(remaining);
            }
            data.copy(this.buffer, 0, dataOffset);
            this.offset = remaining;
        }
        // else: this.offset > 0 && dataOffset === 0 means entire buffer is still partial, offset already correct
    } else {
        this.offset = 0;
        // release memory held by an oversized buffer once fully drained
        if (this.buffer.length > this.options.initialBufferSize) {
            this.buffer = Buffer.allocUnsafe(this.options.initialBufferSize);
        }
    }
};
