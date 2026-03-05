'use strict';

var Connection    = require('./connection');
var Protocol      = require('./protocol');
var errors        = require('./errors');
var utils         = require('./utils');
var Logger        = require('./logger');
var compression   = require('./protocol/misc/compression');
var globals       = require('./protocol/globals');
var url           = require('url');
var fs            = require('fs');
var util          = require('util');
var Kafka         = require('./index');
var promiseUtils  = require('./promise-utils');

function Client(options) {
    var self = this, logger;

    self.options = utils.defaultsDeep(options || {}, {
        clientId: 'no-kafka-client',
        connectionString: process.env.KAFKA_URL || 'kafka://127.0.0.1:9092',
        ssl: {
            cert: process.env.KAFKA_CLIENT_CERT,
            key: process.env.KAFKA_CLIENT_CERT_KEY,
            // secureProtocol: 'TLSv1_method',
            rejectUnauthorized: false,
            // ciphers: 'DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:AES128-SHA256:AES128-SHA:AES256-SHA256:AES256-SHA:RC4-SHA',
            ca: process.env.KAFKA_CLIENT_CA
        },
        asyncCompression: true,
        brokerRedirection: false,
        rebootstrap: false,
        reconnectionDelay: {
            min: 1000,
            max: 1000
        },
        logger: {
            logLevel: 5,
            logstash: {
                enabled: false
            }
        }
    });
    if (!self.validateId(self.options.clientId)) {
        throw new Error('Invalid clientId. Kafka IDs may not contain the following characters: ?:,"');
    }

    logger = new Logger(self.options.logger);

    // prepend clientId argument
    ['log', 'debug', 'error', 'warn', 'trace'].forEach(function (m) {
        self[m] = logger[m].bind(logger, self.options.clientId);
    });

    self.protocol = new Protocol({
        bufferSize: 256 * 1024
    });

    // client metadata
    self.initialBrokers = []; // based on options.connectionString, used for metadata requests
    self.brokerConnections = {};
    self.topicMetadata = {};

    self.correlationId = 0;

    // group metadata
    self.groupCoordinators = {};

    // broker rack info (nodeId -> rack string)
    self.brokerRacks = {};

    // topic ID cache (KIP-516)
    self.topicIds = {};    // topicName -> UUID string
    self.topicNames = {};  // UUID string -> topicName

    // idempotent/transactional state
    self.producerId = -1;
    self.producerEpoch = -1;
    self.transactionCoordinators = {};

    // share group coordinators (KIP-932)
    self.shareCoordinators = {};

    self._updateMetadata_running = Promise.resolve();
    self._updateMetadata_pending = false;
}

module.exports = Client;

function _mapTopics(topics) {
    var result = [], i, j, k, group, tv, p, item, key;
    for (i = 0; i < topics.length; i++) {
        group = Array.isArray(topics[i]) ? topics[i] : [topics[i]];
        for (j = 0; j < group.length; j++) {
            tv = group[j];
            if (tv === null) { continue; } // requiredAcks=0
            for (k = 0; k < tv.partitions.length; k++) {
                p = tv.partitions[k];
                item = { topic: tv.topicName, partition: p.partition };
                for (key in p) {
                    if (key !== 'partition' && p.hasOwnProperty(key)) {
                        item[key] = p[key];
                    }
                }
                result.push(item);
            }
        }
    }
    return result;
}

Client.prototype.init = function () {
    var self = this, p = Promise.resolve(), readFile = util.promisify(fs.readFile);

    // deprecated but backward compatible ssl cert/key options
    if (self.options.ssl.certFile && self.options.ssl.keyFile) {
        self.options.ssl.cert = self.options.ssl.certFile;
        self.options.ssl.key = self.options.ssl.keyFile;
    } else if (self.options.ssl.certStr && self.options.ssl.keyStr) {
        self.options.ssl.cert = self.options.ssl.certStr;
        self.options.ssl.key = self.options.ssl.keyStr;
    } else if (process.env.KAFKA_CLIENT_CERT_STR && process.env.KAFKA_CLIENT_CERT_KEY_STR) {
        self.options.ssl.cert = process.env.KAFKA_CLIENT_CERT_STR;
        self.options.ssl.key = process.env.KAFKA_CLIENT_CERT_KEY_STR;
    }

    if (self.options.ssl.cert && self.options.ssl.key) {
        if (!/^-----BEGIN/.test(self.options.ssl.cert.toString('utf8'))) {
            p = Promise.all([
                readFile(self.options.ssl.cert),
                readFile(self.options.ssl.key)
            ])
            .then(function (r) {
                self.options.ssl.cert = r[0];
                self.options.ssl.key = r[1];
            });
        }
    }

    if (self.options.ssl.ca && !/^-----BEGIN CERTIFICATE-----/.test(self.options.ssl.ca.toString('utf8'))) {
        p = readFile(self.options.ssl.ca).then(function (ca) {
            self.options.ssl.ca = ca;
        });
    }

    return p.then(function () {
        self.initialBrokers = self.options.connectionString.split(',').map(function (hostStr) {
            var parsed, config;

            parsed = self.parseHostString(hostStr);
            config = self.checkBrokerRedirect(parsed.host, parsed.port);

            return config.host && config.port ? self._createConnection(config.host, config.port) : undefined;
        });

        self.initialBrokers = self.initialBrokers.filter(Boolean);

        if (self.initialBrokers.length === 0) {
            throw new Error('No initial hosts to connect');
        }
    })
    .then(function () {
        // discover API versions for each initial broker before first metadata request
        return Promise.all(self.initialBrokers.map(function (conn) {
            return self.apiVersionsRequest(conn);
        }));
    })
    .then(function () {
        return self.updateMetadata();
    });
};

Client.prototype._createConnection = function (host, port) {
    if (host && port) {
        return new Connection({
            host: host,
            port: port,
            ssl: this.options.ssl,
            connectionTimeout: this.options.connectionTimeout,
            socketTimeout: this.options.socketTimeout,
        });
    }

    return undefined;
};

// KIP-899: re-bootstrap when all known brokers are unavailable
Client.prototype._rebootstrap = function () {
    var self = this;

    // close old initialBroker connections
    self.initialBrokers.forEach(function (c) { c.close(); });

    // re-parse connectionString to create fresh connections (DNS re-resolves on connect)
    self.initialBrokers = self.options.connectionString.split(',').map(function (hostStr) {
        var parsed = self.parseHostString(hostStr);
        var config = self.checkBrokerRedirect(parsed.host, parsed.port);
        return config.host && config.port ? self._createConnection(config.host, config.port) : undefined;
    }).filter(Boolean);

    // discover API versions for each new connection
    return Promise.all(self.initialBrokers.map(function (conn) {
        return self.apiVersionsRequest(conn);
    }));
};

Client.prototype.end = function () {
    var self = this;

    self.finished = true;

    return Promise.all(
        Array.prototype.concat(self.initialBrokers, Object.values(self.brokerConnections), Object.values(self.groupCoordinators), Object.values(self.transactionCoordinators), Object.values(self.shareCoordinators))
        .map(function (c) {
            if (c && typeof c.close === 'function') {
                return c.close();
            }
            // handle Promise-wrapped connections
            if (c && typeof c.then === 'function') {
                return c.then(function (conn) { return conn.close(); }).catch(function (err) {
                    self.warn('Failed to close connection during shutdown:', err);
                });
            }
            return null;
        }));
};

Client.prototype.parseHostString = function (hostString) {
    var hostStr = hostString.trim(), parsed;

    // Prepend the protocol, if required
    if (!/^([a-z+]+:)?\/\//.test(hostStr)) {
        hostStr = 'kafka://' + hostStr;
    }
    parsed = url.parse(hostStr);

    return {
        host: parsed.hostname,
        port: parseInt(parsed.port)
    };
};

Client.prototype.checkBrokerRedirect = function (host, port) {
    var fullName, fullNameProtocol;
    var redirect = this.options.brokerRedirection;

    // No remapper
    if (!redirect) {
        return {
            host: host,
            port: port
        };
    }

    // Use a function
    if (typeof redirect === 'function') {
        return redirect(host, port);
    }

    // Name, without protocol
    fullName = host + ':' + port.toString();
    if (redirect[fullName]) {
        return this.parseHostString(redirect[fullName]);
    }

    // Name, with protocol
    fullNameProtocol = 'kafka://' + host + ':' + port.toString();
    if (redirect[fullNameProtocol]) {
        return this.parseHostString(redirect[fullNameProtocol]);
    }

    return {
        host: host,
        port: port
    };
};

Client.prototype.nextCorrelationId = function () {
    if (this.correlationId === 2147483647) {
        this.correlationId = 0;
    }
    return this.correlationId++;
};

Client.prototype.updateMetadata = function (topicNames) {
    var self = this;

    if (self._updateMetadata_pending) {
        return self._updateMetadata_running;
    }

    self._updateMetadata_pending = true;
    self._updateMetadata_running = (function _try() {
        if (self.finished === true) {
            return Promise.resolve(null);
        }

        return self.metadataRequest(topicNames).catch(function (err) {
            // KIP-899: re-bootstrap when all known brokers are unavailable
            if (self.options.rebootstrap) {
                self.warn('All brokers unavailable, attempting rebootstrap from connectionString');
                return self._rebootstrap().then(function () {
                    return self.metadataRequest(topicNames);
                });
            }
            throw err;
        }).then(function (response) {
            var oldConnections = self.brokerConnections;

            self.brokerConnections = {};

            response.broker.forEach(function (broker) {
                var remapped = self.checkBrokerRedirect(broker.host, broker.port);
                var connection, oldKeys = Object.keys(oldConnections), ki;
                for (ki = 0; ki < oldKeys.length; ki++) {
                    if (oldConnections[oldKeys[ki]].equal(remapped.host, remapped.port)) {
                        connection = oldConnections[oldKeys[ki]];
                        delete oldConnections[oldKeys[ki]];
                        break;
                    }
                }

                self.brokerConnections[broker.nodeId] = connection || self._createConnection(remapped.host, remapped.port);
                if (broker.rack) {
                    self.brokerRacks[broker.nodeId] = broker.rack;
                }
            });

            Object.values(oldConnections).forEach(function (c) { c.close(); });

            response.topicMetadata.forEach(function (topic) {
                // KIP-516 v12: resolve null topicName from topicId cache
                if (!topic.topicName && topic.topicId) {
                    topic.topicName = self.topicNames[topic.topicId] || topic.topicId;
                }
                self.topicMetadata[topic.topicName] = {};
                topic.partitionMetadata.forEach(function (partition) {
                    self.topicMetadata[topic.topicName][partition.partitionId] = partition;
                });
                // KIP-516: cache topic IDs from metadata v10+ responses
                if (topic.topicId) {
                    self.topicIds[topic.topicName] = topic.topicId;
                    self.topicNames[topic.topicId] = topic.topicName;
                }
            });

            if (Object.keys(self.brokerConnections).length === 0) {
                self.warn('No broker metadata received, retrying metadata request in 1000ms');
                return promiseUtils.delay(1000).then(_try);
            }

            // discover API versions for each new broker connection
            return Promise.all(Object.values(self.brokerConnections).map(function (conn) {
                if (conn.apiVersions === null) {
                    return self.apiVersionsRequest(conn);
                }
                return null;
            })).then(function () { return null; });
        });
    }())
    .then(function (val) {
        self._updateMetadata_pending = false;
        return val;
    }, function (err) {
        self._updateMetadata_pending = false;
        throw err;
    });

    return self._updateMetadata_running;
};

Client.prototype._waitMetadata = function () {
    var self = this;

    if (self._updateMetadata_pending) {
        return self._updateMetadata_running;
    }

    return Promise.resolve();
};

Client.prototype._sendToAnyBroker = function (correlationId, buffer, parseResponse) {
    var self = this;
    var settled = false;
    var errorsList = [];
    var total = self.initialBrokers.length;

    return new Promise(function (resolve, reject) {
        self.initialBrokers.forEach(function (connection) {
            connection.send(correlationId, buffer).then(function (responseBuffer) {
                return parseResponse(responseBuffer);
            }).then(function (result) {
                if (!settled) {
                    settled = true;
                    self.initialBrokers.forEach(function (c) {
                        c.cancelPending(correlationId);
                    });
                    resolve(result);
                }
            }, function (err) {
                if (settled) { return; } // already resolved — ignore secondary rejections
                errorsList.push(err);
                if (errorsList.length === total) {
                    self.initialBrokers.forEach(function (c) {
                        c.cancelPending(correlationId);
                    });
                    reject(errorsList[0]);
                }
            });
        });
    });
};

Client.prototype.metadataRequest = function (topicNames) {
    var self = this, correlationId = self.nextCorrelationId();
    var metadataMax = 0;
    var buffer, responseName;

    // check max metadata version supported by any initial broker
    self.initialBrokers.forEach(function (conn) {
        if (conn.apiVersions && conn.apiVersions[globals.API_KEYS.MetadataRequest]) {
            metadataMax = Math.max(metadataMax, conn.apiVersions[globals.API_KEYS.MetadataRequest].max);
        }
    });
    if (metadataMax >= 13) {
        buffer = self.protocol.write().MetadataRequestV13({
            correlationId: correlationId,
            clientId: self.options.clientId,
            topicNames: (topicNames || []).map(function (name) {
                return { topicId: self.topicIds[name] || null, name: name };
            })
        }).result;
        responseName = 'MetadataResponseV13';
    } else if (metadataMax >= 12) {
        buffer = self.protocol.write().MetadataRequestV12({
            correlationId: correlationId,
            clientId: self.options.clientId,
            topicNames: (topicNames || []).map(function (name) {
                return { topicId: self.topicIds[name] || null, name: name };
            })
        }).result;
        responseName = 'MetadataResponseV12';
    } else if (metadataMax >= 11) {
        buffer = self.protocol.write().MetadataRequestV11({
            correlationId: correlationId,
            clientId: self.options.clientId,
            topicNames: (topicNames || []).map(function (name) {
                return { topicId: null, name: name };
            })
        }).result;
        responseName = 'MetadataResponseV11';
    } else if (metadataMax >= 10) {
        buffer = self.protocol.write().MetadataRequestV10({
            correlationId: correlationId,
            clientId: self.options.clientId,
            topicNames: (topicNames || []).map(function (name) {
                return { topicId: null, name: name };
            })
        }).result;
        responseName = 'MetadataResponseV10';
    } else if (metadataMax >= 9) {
        buffer = self.protocol.write().MetadataRequestV9({
            correlationId: correlationId,
            clientId: self.options.clientId,
            topicNames: topicNames || []
        }).result;
        responseName = 'MetadataResponseV9';
    } else if (metadataMax >= 8) {
        buffer = self.protocol.write().MetadataRequestV8({
            correlationId: correlationId,
            clientId: self.options.clientId,
            topicNames: topicNames || []
        }).result;
        responseName = 'MetadataResponseV8';
    } else if (metadataMax >= 7) {
        buffer = self.protocol.write().MetadataRequestV7({
            correlationId: correlationId,
            clientId: self.options.clientId,
            topicNames: topicNames || []
        }).result;
        responseName = 'MetadataResponseV7';
    } else if (metadataMax >= 5) {
        buffer = self.protocol.write().MetadataRequestV5({
            correlationId: correlationId,
            clientId: self.options.clientId,
            topicNames: topicNames || []
        }).result;
        responseName = 'MetadataResponseV5';
    } else if (metadataMax >= 1) {
        buffer = self.protocol.write().MetadataRequestV1({
            correlationId: correlationId,
            clientId: self.options.clientId,
            topicNames: topicNames || []
        }).result;
        responseName = 'MetadataResponseV1';
    } else {
        buffer = self.protocol.write().MetadataRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            topicNames: topicNames || []
        }).result;
        responseName = 'MetadataResponse';
    }

    return self._sendToAnyBroker(correlationId, buffer, function (responseBuffer) {
        var result = self.protocol.read(responseBuffer)[responseName]().result;
        if (result.controllerId !== undefined) {
            self.controllerId = result.controllerId;
        }
        if (result.clusterId !== undefined) {
            self.clusterId = result.clusterId;
        }
        return result;
    });
};

Client.prototype.getTopicPartitions = function (topic) {
    var self = this;

    function _try() {
        if (self.topicMetadata.hasOwnProperty(topic)) {
            return self.topicMetadata[topic];
        }
        throw errors.byName('UnknownTopicOrPartition');
    }

    return self._waitMetadata().then(_try)
    .catch(function (err) {
        if (err && err.code === 'UnknownTopicOrPartition') {
            return self.updateMetadata([topic]).then(_try);
        }
        throw err;
    })
    .then(function (obj) { return Object.values(obj); });
};

Client.prototype.findLeader = function (topic, partition) {
    var self = this;

    function _try() {
        var tm = self.topicMetadata[topic], r = -1;
        if (tm && tm[partition] && tm[partition].leader !== undefined) {
            r = tm[partition].leader;
        }
        if (r === -1) {
            throw errors.byName('UnknownTopicOrPartition');
        }
        if (!self.brokerConnections[r]) {
            throw errors.byName('LeaderNotAvailable');
        }
        return r;
    }

    return self._waitMetadata().then(_try)
    .catch(function (err) {
        if (err && (err.code === 'UnknownTopicOrPartition' || err.code === 'LeaderNotAvailable')) {
            return self.updateMetadata([topic]).then(_try);
        }
        throw err;
    });
};

Client.prototype.leaderServer = function (leader) {
    var conn = this.brokerConnections[leader];
    return conn && typeof conn.server === 'function' ? conn.server() : '-';
};

function _fakeTopicsErrorResponse(topics, error) {
    return topics.map(function (t) {
        return {
            topicName: t.topicName,
            partitions: t.partitions.map(function (p) {
                return {
                    partition: p.partition,
                    error: error
                };
            })
        };
    });
}

Client.prototype.produceRequest = function (requests, codec, compressionLevel) {
    var self = this, compressionPromises = [];

    var compress = self.options.asyncCompression ? compression.compressAsync : compression.compress;

    function processPartition(pv, pk, leader) {
        var conn = self.brokerConnections[leader];
        var produceVersion = conn ? self._negotiateVersion(conn, globals.API_KEYS.ProduceRequest, 13) : 0;
        var partition = parseInt(pk);
        var _r, recordsList, firstTs, maxTs, j, recTs, tmpWriter, recordsBuf;

        if (produceVersion >= 3) {
            // RecordBatch v2 format
            recordsList = pv.map(function (mv) {
                return {
                    key: mv.message.key,
                    value: mv.message.value,
                    timestamp: mv.message.timestamp,
                    headers: mv.message.headers
                };
            });

            _r = {
                partition: partition,
                batch: {
                    baseOffset: 0,
                    records: recordsList,
                    codec: codec
                }
            };

            if (self.producerId >= 0) {
                _r.batch.producerId = self.producerId;
                _r.batch.producerEpoch = self.producerEpoch;
                _r.batch.baseSequence = pv[0]._baseSequence !== null && pv[0]._baseSequence !== undefined ? pv[0]._baseSequence : -1;
                if (self.options.transactionalId) {
                    _r.batch.isTransactional = true;
                }
            }

            if (codec > 0) {
                // serialize records to a buffer, then compress
                firstTs = Date.now();
                maxTs = firstTs;
                tmpWriter = self.protocol.write();
                for (j = 0; j < recordsList.length; j++) {
                    recTs = recordsList[j].timestamp || firstTs;
                    if (recTs > maxTs) { maxTs = recTs; }
                    tmpWriter.Record({
                        attributes: 0,
                        timestampDelta: recTs - firstTs,
                        offsetDelta: j,
                        key: recordsList[j].key,
                        value: recordsList[j].value,
                        headers: recordsList[j].headers || []
                    });
                }
                recordsBuf = Buffer.from(tmpWriter.result);

                compressionPromises.push(compress(recordsBuf, codec, compressionLevel).then(function (compressed) {
                    _r.batch.compressedRecords = compressed;
                    _r.batch.firstTimestamp = firstTs;
                    _r.batch.maxTimestamp = maxTs;
                    _r.batch.recordCount = recordsList.length;
                    _r.batch.lastOffsetDelta = recordsList.length > 0 ? recordsList.length - 1 : 0;
                    _r.batch.records = [];
                })
                .catch(function (err) {
                    self.warn('Failed to compress RecordBatch', err);
                    _r.batch.codec = 0; // fall back to uncompressed
                }));
            }

            return _r;
        }

        // legacy MessageSet format
        _r = {
            partition: partition,
            messageSet: []
        };
        compressionPromises.push(self._compressMessageSet(pv.map(function (mv) {
            return { offset: 0, message: mv.message };
        }), codec, compressionLevel).then(function (messageSet) {
            _r.messageSet = messageSet;
        }));
        return _r;
    }

    return self._waitMetadata().then(function () {
        var leaderGroups = utils.groupBy(requests, function (r) { return r.leader; });
        requests = {};
        Object.keys(leaderGroups).forEach(function (leader) {
            var topicGroups = utils.groupBy(leaderGroups[leader], function (r) { return r.topic; });
            requests[leader] = Object.keys(topicGroups).map(function (t) {
                var partGroups = utils.groupBy(topicGroups[t], function (r) { return r.partition; });
                return {
                    topicName: t,
                    partitions: Object.keys(partGroups).map(function (pk) {
                        return processPartition(partGroups[pk], pk, leader);
                    })
                };
            });
        });

        return Promise.all(compressionPromises).then(function () {
            return Promise.all(Object.keys(requests).map(function (leader) {
                var topics = requests[leader];
                var correlationId = self.nextCorrelationId();
                var conn = self.brokerConnections[leader];
                var produceVersion = conn ? self._negotiateVersion(conn, globals.API_KEYS.ProduceRequest, 13) : 0;
                var buffer, responseName;

                if (produceVersion >= 13) {
                    buffer = self.protocol.write().ProduceRequestV13({
                        correlationId: correlationId,
                        clientId: self.options.clientId,
                        transactionalId: self.options.transactionalId || null,
                        requiredAcks: self.options.requiredAcks,
                        timeout: self.options.timeout,
                        topics: topics.map(function (t) {
                            return { topicId: self.topicIds[t.topicName] || null, partitions: t.partitions };
                        })
                    }).result;
                    responseName = 'ProduceResponseV13';
                } else if (produceVersion >= 12) {
                    buffer = self.protocol.write().ProduceRequestV12({
                        correlationId: correlationId,
                        clientId: self.options.clientId,
                        transactionalId: self.options.transactionalId || null,
                        requiredAcks: self.options.requiredAcks,
                        timeout: self.options.timeout,
                        topics: topics
                    }).result;
                    responseName = 'ProduceResponseV10'; // v12 response same as v10
                } else if (produceVersion >= 11) {
                    buffer = self.protocol.write().ProduceRequestV11({
                        correlationId: correlationId,
                        clientId: self.options.clientId,
                        transactionalId: self.options.transactionalId || null,
                        requiredAcks: self.options.requiredAcks,
                        timeout: self.options.timeout,
                        topics: topics
                    }).result;
                    responseName = 'ProduceResponseV10'; // v11 response same wire format as v10
                } else if (produceVersion >= 10) {
                    buffer = self.protocol.write().ProduceRequestV10({
                        correlationId: correlationId,
                        clientId: self.options.clientId,
                        transactionalId: self.options.transactionalId || null,
                        requiredAcks: self.options.requiredAcks,
                        timeout: self.options.timeout,
                        topics: topics
                    }).result;
                    responseName = 'ProduceResponseV10';
                } else if (produceVersion >= 9) {
                    buffer = self.protocol.write().ProduceRequestV9({
                        correlationId: correlationId,
                        clientId: self.options.clientId,
                        transactionalId: self.options.transactionalId || null,
                        requiredAcks: self.options.requiredAcks,
                        timeout: self.options.timeout,
                        topics: topics
                    }).result;
                    responseName = 'ProduceResponseV9';
                } else if (produceVersion >= 8) {
                    buffer = self.protocol.write().ProduceRequestV8({
                        correlationId: correlationId,
                        clientId: self.options.clientId,
                        transactionalId: self.options.transactionalId || null,
                        requiredAcks: self.options.requiredAcks,
                        timeout: self.options.timeout,
                        topics: topics
                    }).result;
                    responseName = 'ProduceResponseV7'; // v8 response same wire format as v7
                } else if (produceVersion >= 7) {
                    buffer = self.protocol.write().ProduceRequestV7({
                        correlationId: correlationId,
                        clientId: self.options.clientId,
                        transactionalId: self.options.transactionalId || null,
                        requiredAcks: self.options.requiredAcks,
                        timeout: self.options.timeout,
                        topics: topics
                    }).result;
                    responseName = 'ProduceResponseV5'; // v7 response same wire format as v5
                } else if (produceVersion >= 5) {
                    buffer = self.protocol.write().ProduceRequestV5({
                        correlationId: correlationId,
                        clientId: self.options.clientId,
                        transactionalId: self.options.transactionalId || null,
                        requiredAcks: self.options.requiredAcks,
                        timeout: self.options.timeout,
                        topics: topics
                    }).result;
                    responseName = 'ProduceResponseV5';
                } else if (produceVersion >= 3) {
                    buffer = self.protocol.write().ProduceRequestV3({
                        correlationId: correlationId,
                        clientId: self.options.clientId,
                        transactionalId: self.options.transactionalId || null,
                        requiredAcks: self.options.requiredAcks,
                        timeout: self.options.timeout,
                        topics: topics
                    }).result;
                    responseName = 'ProduceResponseV2';
                } else {
                    buffer = self.protocol.write().ProduceRequest({
                        correlationId: correlationId,
                        clientId: self.options.clientId,
                        requiredAcks: self.options.requiredAcks,
                        timeout: self.options.timeout,
                        topics: topics
                    }).result;
                    responseName = 'ProduceResponse';
                }

                return conn.send(correlationId, buffer, self.options.requiredAcks === 0).then(function (responseBuffer) {
                    var result;
                    if (self.options.requiredAcks !== 0) {
                        result = self.protocol.read(responseBuffer)[responseName]().result;
                        // v13 returns topicId instead of topicName — resolve from cache
                        if (produceVersion >= 13) {
                            result.topics.forEach(function (t) {
                                t.topicName = self.topicNames[t.topicId] || t.topicId;
                            });
                        }
                        if (produceVersion >= 10) {
                            self._applyLeaderHints(result.topics, result.nodeEndpoints);
                        }
                        return result.topics;
                    }
                    return null;
                })
                .catch(function (err) {
                    if (err instanceof errors.NoKafkaConnectionError) {
                        return _fakeTopicsErrorResponse(topics, err);
                    }
                    throw err;
                });
            }));
        })
        .then(_mapTopics);
    });
};

Client.prototype.fetchRequest = function (requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return Promise.all(Object.keys(requests).map(function (leader) {
            var topics = requests[leader];
            var buffer, correlationId = self.nextCorrelationId(), conn, fetchVersion, responseName;
            // fake LeaderNotAvailable for all topics with no leader
            if (leader === -1 || !self.brokerConnections[leader]) {
                return _fakeTopicsErrorResponse(topics, errors.byName('LeaderNotAvailable'));
            }

            conn = self.brokerConnections[leader];
            fetchVersion = self._negotiateVersion(conn, globals.API_KEYS.FetchRequest, 16);

            if (fetchVersion >= 16) {
                // KIP-951: response adds NodeEndpoints
                buffer = self.protocol.write().FetchRequestV16({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    maxBytes: self.options.maxBytes || 0x7fffffff,
                    isolationLevel: self.options.isolationLevel || 0,
                    sessionId: 0,
                    sessionEpoch: -1,
                    topics: topics.map(function (t) {
                        return {
                            topicId: self.topicIds[t.topicName] || null,
                            partitions: t.partitions
                        };
                    }),
                    forgottenTopicsData: [],
                    rackId: self.options.rackId || ''
                }).result;
                responseName = 'FetchResponseV16';
            } else if (fetchVersion >= 15) {
                // KIP-903: removes replicaId from request
                buffer = self.protocol.write().FetchRequestV15({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    maxBytes: self.options.maxBytes || 0x7fffffff,
                    isolationLevel: self.options.isolationLevel || 0,
                    sessionId: 0,
                    sessionEpoch: -1,
                    topics: topics.map(function (t) {
                        return {
                            topicId: self.topicIds[t.topicName] || null,
                            partitions: t.partitions
                        };
                    }),
                    forgottenTopicsData: [],
                    rackId: self.options.rackId || ''
                }).result;
                responseName = 'FetchResponseV13';
            } else if (fetchVersion >= 14) {
                // KIP-405: tiered storage version bump
                buffer = self.protocol.write().FetchRequestV14({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    maxBytes: self.options.maxBytes || 0x7fffffff,
                    isolationLevel: self.options.isolationLevel || 0,
                    sessionId: 0,
                    sessionEpoch: -1,
                    topics: topics.map(function (t) {
                        return {
                            topicId: self.topicIds[t.topicName] || null,
                            partitions: t.partitions
                        };
                    }),
                    forgottenTopicsData: [],
                    rackId: self.options.rackId || ''
                }).result;
                responseName = 'FetchResponseV13';
            } else if (fetchVersion >= 13) {
                // KIP-516: use topicId instead of topic name
                buffer = self.protocol.write().FetchRequestV13({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    maxBytes: self.options.maxBytes || 0x7fffffff,
                    isolationLevel: self.options.isolationLevel || 0,
                    sessionId: 0,
                    sessionEpoch: -1,
                    topics: topics.map(function (t) {
                        return {
                            topicId: self.topicIds[t.topicName] || null,
                            partitions: t.partitions
                        };
                    }),
                    forgottenTopicsData: [],
                    rackId: self.options.rackId || ''
                }).result;
                responseName = 'FetchResponseV13';
            } else if (fetchVersion >= 12) {
                buffer = self.protocol.write().FetchRequestV12({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    maxBytes: self.options.maxBytes || 0x7fffffff,
                    isolationLevel: self.options.isolationLevel || 0,
                    sessionId: 0,
                    sessionEpoch: -1,
                    topics: topics,
                    forgottenTopicsData: [],
                    rackId: self.options.rackId || ''
                }).result;
                responseName = 'FetchResponseV12';
            } else if (fetchVersion >= 11) {
                buffer = self.protocol.write().FetchRequestV11({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    maxBytes: self.options.maxBytes || 0x7fffffff,
                    isolationLevel: self.options.isolationLevel || 0,
                    sessionId: 0,
                    sessionEpoch: -1,
                    topics: topics,
                    forgottenTopicsData: [],
                    rackId: self.options.rackId || ''
                }).result;
                responseName = 'FetchResponseV11';
            } else if (fetchVersion >= 9) {
                buffer = self.protocol.write().FetchRequestV10({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    maxBytes: self.options.maxBytes || 0x7fffffff,
                    isolationLevel: self.options.isolationLevel || 0,
                    sessionId: 0,
                    sessionEpoch: -1,
                    topics: topics,
                    forgottenTopicsData: []
                }).result;
                responseName = 'FetchResponseV7';
            } else if (fetchVersion >= 7) {
                buffer = self.protocol.write().FetchRequestV7({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    maxBytes: self.options.maxBytes || 0x7fffffff,
                    isolationLevel: self.options.isolationLevel || 0,
                    sessionId: 0,
                    sessionEpoch: -1,
                    topics: topics,
                    forgottenTopicsData: []
                }).result;
                responseName = 'FetchResponseV7';
            } else if (fetchVersion >= 5) {
                buffer = self.protocol.write().FetchRequestV6({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    maxBytes: self.options.maxBytes || 0x7fffffff,
                    isolationLevel: self.options.isolationLevel || 0,
                    topics: topics
                }).result;
                responseName = 'FetchResponseV5';
            } else if (fetchVersion >= 4) {
                buffer = self.protocol.write().FetchRequestV4({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    maxBytes: self.options.maxBytes || 0x7fffffff,
                    isolationLevel: self.options.isolationLevel || 0,
                    topics: topics
                }).result;
                responseName = 'FetchResponseV4';
            } else if (fetchVersion >= 3) {
                buffer = self.protocol.write().FetchRequestV3({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    maxBytes: self.options.maxBytes || 0x7fffffff,
                    topics: topics
                }).result;
                responseName = 'FetchResponseV3';
            } else {
                buffer = self.protocol.write().FetchRequest({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    maxWaitTime: self.options.maxWaitTime,
                    minBytes: self.options.minBytes,
                    topics: topics
                }).result;
                responseName = 'FetchResponse';
            }

            return conn.send(correlationId, buffer).then(function (responseBuffer) {
                var parsed = self.protocol.read(responseBuffer)[responseName]().result;
                var result = parsed.topics;
                // KIP-516: resolve topicId back to topicName for v13+ responses
                if (fetchVersion >= 13) {
                    result.forEach(function (t) {
                        t.topicName = self.topicNames[t.topicId] || t.topicId;
                    });
                }
                // KIP-951: apply leader hints from v12+ responses
                if (fetchVersion >= 12) {
                    self._applyLeaderHints(result, parsed.nodeEndpoints || null);
                }
                return result;
            })
            .catch(function (err) {
                if (err instanceof errors.NoKafkaConnectionError) {
                    return _fakeTopicsErrorResponse(topics, err);
                }
                throw err;
            });
        }))
        .then(function (topics) {
            return Promise.all(_mapTopics(topics).map(function (r) {
                var messageSet = r.messageSet || [];

                // check if messageSet contains RecordBatch entries (from v4 response)
                if (messageSet.length > 0 && messageSet[0].header && messageSet[0].recordsRaw) {
                    return self._decompressRecordBatches(messageSet).then(function (converted) {
                        return Object.assign({}, r, { messageSet: converted });
                    });
                }

                return Promise.all(messageSet.map(function (m) {
                    if (m.message.attributes.codec === 0) {
                        return Promise.resolve(m);
                    }

                    return self._decompressMessageSet(m.message).catch(function (err) {
                        self.error('Failed to decompress message at', r.topic + ':' + r.partition + '@' + m.offset, err);
                    });
                }))
                .then(function (newMessageSet) {
                    return Object.assign({}, r, { messageSet: [].concat.apply([], newMessageSet) });
                });
            }));
        });
    });
};

Client.prototype.getPartitionOffset = function (leader, topic, partition, time) {
    var self = this;
    var request = {};
    var conn = self.brokerConnections[leader];
    var offsetVersion = conn ? self._negotiateVersion(conn, globals.API_KEYS.OffsetRequest, 6) : 0;

    request[leader] = [{
        topicName: topic,
        partitions: [{
            partition: partition,
            time: time || Kafka.LATEST_OFFSET, // the latest (next) offset by default
            maxNumberOfOffsets: 1
        }]
    }];

    return this.offsetRequest(request).then(function (result) {
        var p = result[0];
        if (p.error) {
            throw p.error;
        }
        // v1 returns single offset, v0 returns offset array
        if (offsetVersion >= 1) {
            return p.offset;
        }
        return p.offset[0];
    });
};

Client.prototype.offsetRequest = function (requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return Promise.all(Object.keys(requests).map(function (leader) {
            var topics = requests[leader];
            var correlationId = self.nextCorrelationId();
            var conn = self.brokerConnections[leader];
            var offsetVersion = conn ? self._negotiateVersion(conn, globals.API_KEYS.OffsetRequest, 10) : 0;
            var buffer, responseName;

            if (offsetVersion >= 10) {
                buffer = self.protocol.write().OffsetRequestV10({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    isolationLevel: self.options.isolationLevel || 0,
                    topics: topics,
                    timeoutMs: 0
                }).result;
                responseName = 'OffsetResponseV6'; // v10 response same as v6
            } else if (offsetVersion >= 9) {
                buffer = self.protocol.write().OffsetRequestV9({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    isolationLevel: self.options.isolationLevel || 0,
                    topics: topics
                }).result;
                responseName = 'OffsetResponseV6';
            } else if (offsetVersion >= 8) {
                buffer = self.protocol.write().OffsetRequestV8({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    isolationLevel: self.options.isolationLevel || 0,
                    topics: topics
                }).result;
                responseName = 'OffsetResponseV6';
            } else if (offsetVersion >= 7) {
                buffer = self.protocol.write().OffsetRequestV7({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    isolationLevel: self.options.isolationLevel || 0,
                    topics: topics
                }).result;
                responseName = 'OffsetResponseV6';
            } else if (offsetVersion >= 6) {
                buffer = self.protocol.write().OffsetRequestV6({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    isolationLevel: self.options.isolationLevel || 0,
                    topics: topics
                }).result;
                responseName = 'OffsetResponseV6';
            } else if (offsetVersion >= 5) {
                buffer = self.protocol.write().OffsetRequestV5({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    isolationLevel: self.options.isolationLevel || 0,
                    topics: topics
                }).result;
                responseName = 'OffsetResponseV4'; // v5 response same as v4
            } else if (offsetVersion >= 4) {
                buffer = self.protocol.write().OffsetRequestV4({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    isolationLevel: self.options.isolationLevel || 0,
                    topics: topics
                }).result;
                responseName = 'OffsetResponseV4';
            } else if (offsetVersion >= 2) {
                buffer = self.protocol.write().OffsetRequestV2({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    isolationLevel: self.options.isolationLevel || 0,
                    topics: topics
                }).result;
                responseName = 'OffsetResponseV2'; // v2 response adds throttleTime
            } else if (offsetVersion >= 1) {
                buffer = self.protocol.write().OffsetRequestV1({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    topics: topics
                }).result;
                responseName = 'OffsetResponseV1';
            } else {
                buffer = self.protocol.write().OffsetRequest({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    topics: topics
                }).result;
                responseName = 'OffsetResponse';
            }

            return conn.send(correlationId, buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer)[responseName]().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};

Client.prototype.offsetCommitRequestV0 = function (groupId, requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return Promise.all(Object.keys(requests).map(function (leader) {
            var topics = requests[leader];
            var correlationId = self.nextCorrelationId();
            var buffer = self.protocol.write().OffsetCommitRequestV0({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                topics: topics
            }).result;

            return self.brokerConnections[leader].send(correlationId, buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).OffsetCommitResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};


Client.prototype.offsetFetchRequestV0 = function (groupId, requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return Promise.all(Object.keys(requests).map(function (leader) {
            var topics = requests[leader];
            var correlationId = self.nextCorrelationId();
            var buffer = self.protocol.write().OffsetFetchRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                apiVersion: 0,
                groupId: groupId,
                topics: topics
            }).result;

            return self.brokerConnections[leader].send(correlationId, buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer).OffsetFetchResponse().result.topics;
            });
        }))
        .then(_mapTopics);
    });
};

Client.prototype.offsetFetchRequestV1 = function (groupId, requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return self._findGroupCoordinator(groupId).then(function (connection) {
            var correlationId = self.nextCorrelationId();
            var fetchVersion = self._negotiateVersion(connection, globals.API_KEYS.OffsetFetchRequest, 10);
            var buffer, responseName, group;

            if (fetchVersion >= 10) {
                buffer = self.protocol.write().OffsetFetchRequestV10({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groups: [{
                        groupId: groupId,
                        topics: requests ? requests.map(function (t) {
                            return { topicId: self.topicIds[t.topicName] || null, partitions: t.partitions };
                        }) : null
                    }],
                    requireStable: false
                }).result;
                responseName = 'OffsetFetchResponseV10';
            } else if (fetchVersion >= 8) {
                buffer = self.protocol.write().OffsetFetchRequestV8({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    apiVersion: fetchVersion,
                    groups: [{ groupId: groupId, topics: requests, apiVersion: fetchVersion }],
                    requireStable: false
                }).result;
                responseName = 'OffsetFetchResponseV8';
            } else if (fetchVersion >= 7) {
                buffer = self.protocol.write().OffsetFetchRequestV7({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groupId: groupId,
                    topics: requests,
                    requireStable: false
                }).result;
                responseName = 'OffsetFetchResponseV6';
            } else if (fetchVersion >= 6) {
                buffer = self.protocol.write().OffsetFetchRequestV6({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groupId: groupId,
                    topics: requests
                }).result;
                responseName = 'OffsetFetchResponseV6';
            } else if (fetchVersion >= 5) {
                buffer = self.protocol.write().OffsetFetchRequestV5({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groupId: groupId,
                    topics: requests
                }).result;
                responseName = 'OffsetFetchResponseV5';
            } else if (fetchVersion >= 3) {
                buffer = self.protocol.write().OffsetFetchRequestV3({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groupId: groupId,
                    topics: requests
                }).result;
                responseName = 'OffsetFetchResponseV3';
            } else if (fetchVersion >= 2) {
                buffer = self.protocol.write().OffsetFetchRequest({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    apiVersion: 2,
                    groupId: groupId,
                    topics: requests
                }).result;
                responseName = 'OffsetFetchResponseV2';
            } else {
                buffer = self.protocol.write().OffsetFetchRequest({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    apiVersion: 1,
                    groupId: groupId,
                    topics: requests
                }).result;
                responseName = 'OffsetFetchResponse';
            }

            return connection.send(correlationId, buffer).then(function (responseBuffer) {
                var result = self.protocol.read(responseBuffer)[responseName]().result;
                if (result.groups) {
                    // v8+ response: extract first group from groups array
                    group = result.groups[0];
                    if (group.error) {
                        throw group.error;
                    }
                    // v10 returns topicId instead of topicName — resolve from cache
                    if (fetchVersion >= 10) {
                        group.topics.forEach(function (t) {
                            t.topicName = self.topicNames[t.topicId] || t.topicId;
                        });
                    }
                    return group.topics;
                }
                if (result.error) {
                    throw result.error;
                }
                return result.topics;
            });
        })
        .then(_mapTopics);
    });
};

// close coordinator connection if 'NotCoordinatorForGroup' received
// not sure about 'GroupCoordinatorNotAvailable' or 'GroupLoadInProgress'..
Client.prototype.updateGroupCoordinator = function (groupId) {
    var self = this;
    if (self.groupCoordinators[groupId]) {
        return self.groupCoordinators[groupId].then(function (connection) {
            connection.close();
            delete self.groupCoordinators[groupId];
        })
        .catch(function (err) {
            self.warn('Failed to close group coordinator for', groupId, err);
            delete self.groupCoordinators[groupId];
        });
    }
    delete self.groupCoordinators[groupId];
    return Promise.resolve();
};

Client.prototype.updateTransactionCoordinator = function (transactionalId) {
    var self = this;
    if (self.transactionCoordinators[transactionalId]) {
        return self.transactionCoordinators[transactionalId].then(function (connection) {
            connection.close();
            delete self.transactionCoordinators[transactionalId];
        })
        .catch(function (err) {
            self.warn('Failed to close transaction coordinator for', transactionalId, err);
            delete self.transactionCoordinators[transactionalId];
        });
    }
    delete self.transactionCoordinators[transactionalId];
    return Promise.resolve();
};

Client.prototype._findGroupCoordinator = function (groupId) {
    var self = this, buffer, correlationId = self.nextCorrelationId();
    var coordMax = 0, responseName;

    if (self.groupCoordinators[groupId]) {
        return self.groupCoordinators[groupId];
    }

    // check max FindCoordinator version across initial brokers
    self.initialBrokers.forEach(function (conn) {
        if (conn.apiVersions && conn.apiVersions[globals.API_KEYS.GroupCoordinatorRequest]) {
            coordMax = Math.max(coordMax, conn.apiVersions[globals.API_KEYS.GroupCoordinatorRequest].max);
        }
    });

    if (coordMax >= 4) {
        buffer = self.protocol.write().FindCoordinatorRequestV4({
            correlationId: correlationId,
            clientId: self.options.clientId,
            apiVersion: Math.min(coordMax, 6),
            keyType: 0,
            coordinatorKeys: [groupId]
        }).result;
        responseName = 'FindCoordinatorResponseV4';
    } else if (coordMax >= 3) {
        buffer = self.protocol.write().FindCoordinatorRequestV3({
            correlationId: correlationId,
            clientId: self.options.clientId,
            key: groupId,
            coordinatorType: 0
        }).result;
        responseName = 'FindCoordinatorResponseV3';
    } else if (coordMax >= 2) {
        buffer = self.protocol.write().FindCoordinatorRequestV2({
            correlationId: correlationId,
            clientId: self.options.clientId,
            key: groupId,
            coordinatorType: 0
        }).result;
        responseName = 'FindCoordinatorResponse';
    } else if (coordMax >= 1) {
        buffer = self.protocol.write().FindCoordinatorRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            key: groupId,
            coordinatorType: 0
        }).result;
        responseName = 'FindCoordinatorResponse';
    } else {
        buffer = self.protocol.write().GroupCoordinatorRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            groupId: groupId
        }).result;
        responseName = 'GroupCoordinatorResponse';
    }

    self.groupCoordinators[groupId] = self._sendToAnyBroker(correlationId, buffer, function (responseBuffer) {
        var result = self.protocol.read(responseBuffer)[responseName]().result;
        var coord;
        if (result.coordinators) {
            coord = result.coordinators[0];
            if (coord.error) {
                throw coord.error;
            }
            return { coordinatorHost: coord.host, coordinatorPort: coord.port, coordinatorId: coord.nodeId };
        }
        if (result.error) {
            throw result.error;
        }
        return result;
    })
    .then(function (host) {
        var remapped = self.checkBrokerRedirect(host.coordinatorHost, host.coordinatorPort);
        var conn = self._createConnection(remapped.host, remapped.port);
        return self.apiVersionsRequest(conn).then(function () { return conn; });
    })
    .catch(function (err) {
        delete self.groupCoordinators[groupId];
        throw err;
    });

    return self.groupCoordinators[groupId];
};

Client.prototype.joinConsumerGroupRequest = function (groupId, memberId, sessionTimeout, strategies, rebalanceTimeout) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var joinVersion = self._negotiateVersion(connection, globals.API_KEYS.JoinGroupRequest, 9);
        var buffer, responseName;

        if (joinVersion >= 8) {
            buffer = self.protocol.write()[joinVersion >= 9 ? 'JoinConsumerGroupRequestV9' : 'JoinConsumerGroupRequestV8']({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                sessionTimeout: sessionTimeout,
                rebalanceTimeout: rebalanceTimeout !== undefined ? rebalanceTimeout : sessionTimeout,
                memberId: memberId || '',
                groupInstanceId: self.options.groupInstanceId || null,
                groupProtocols: strategies,
                reason: ''
            }).result;
            responseName = joinVersion >= 9 ? 'JoinConsumerGroupResponseV9' : 'JoinConsumerGroupResponseV7';
        } else if (joinVersion >= 7) {
            buffer = self.protocol.write().JoinConsumerGroupRequestV7({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                sessionTimeout: sessionTimeout,
                rebalanceTimeout: rebalanceTimeout !== undefined ? rebalanceTimeout : sessionTimeout,
                memberId: memberId || '',
                groupInstanceId: self.options.groupInstanceId || null,
                groupProtocols: strategies
            }).result;
            responseName = 'JoinConsumerGroupResponseV7';
        } else if (joinVersion >= 6) {
            buffer = self.protocol.write().JoinConsumerGroupRequestV6({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                sessionTimeout: sessionTimeout,
                rebalanceTimeout: rebalanceTimeout !== undefined ? rebalanceTimeout : sessionTimeout,
                memberId: memberId || '',
                groupInstanceId: self.options.groupInstanceId || null,
                groupProtocols: strategies
            }).result;
            responseName = 'JoinConsumerGroupResponseV6';
        } else if (joinVersion >= 5) {
            buffer = self.protocol.write().JoinConsumerGroupRequestV5({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                sessionTimeout: sessionTimeout,
                rebalanceTimeout: rebalanceTimeout !== undefined ? rebalanceTimeout : sessionTimeout,
                memberId: memberId || '',
                groupInstanceId: self.options.groupInstanceId || null,
                groupProtocols: strategies
            }).result;
            responseName = 'JoinConsumerGroupResponseV5';
        } else if (joinVersion >= 3) {
            buffer = self.protocol.write().JoinConsumerGroupRequestV3({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                sessionTimeout: sessionTimeout,
                rebalanceTimeout: rebalanceTimeout !== undefined ? rebalanceTimeout : sessionTimeout,
                memberId: memberId || '',
                groupProtocols: strategies
            }).result;
            responseName = 'JoinConsumerGroupResponseV3';
        } else if (joinVersion >= 1) {
            buffer = self.protocol.write().JoinConsumerGroupRequestV1({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                sessionTimeout: sessionTimeout,
                rebalanceTimeout: rebalanceTimeout !== undefined ? rebalanceTimeout : sessionTimeout,
                memberId: memberId || '',
                groupProtocols: strategies
            }).result;
            responseName = 'JoinConsumerGroupResponse';
        } else {
            buffer = self.protocol.write().JoinConsumerGroupRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                sessionTimeout: sessionTimeout,
                memberId: memberId || '',
                groupProtocols: strategies
            }).result;
            responseName = 'JoinConsumerGroupResponse';
        }

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer)[responseName]().result;
            if (result.error) {
                // KIP-394: broker returns MemberIdRequired with the assigned memberId
                if (result.error.code === 'MemberIdRequired' && result.memberId) {
                    result.error.memberId = result.memberId;
                }
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.heartbeatRequest = function (groupId, memberId, generationId) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var hbVersion = self._negotiateVersion(connection, globals.API_KEYS.HeartbeatRequest, 4);
        var buffer, responseName;

        if (hbVersion >= 4) {
            buffer = self.protocol.write().HeartbeatRequestV4({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                generationId: generationId,
                groupInstanceId: self.options.groupInstanceId || null
            }).result;
            responseName = 'HeartbeatResponseV4';
        } else if (hbVersion >= 3) {
            buffer = self.protocol.write().HeartbeatRequestV3({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                generationId: generationId,
                groupInstanceId: self.options.groupInstanceId || null
            }).result;
            responseName = 'HeartbeatResponseV1'; // v3 response same as v1
        } else if (hbVersion >= 2) {
            buffer = self.protocol.write().HeartbeatRequestV2({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                generationId: generationId
            }).result;
            responseName = 'HeartbeatResponseV1'; // v2 response same as v1
        } else if (hbVersion >= 1) {
            buffer = self.protocol.write().HeartbeatRequestV1({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                generationId: generationId
            }).result;
            responseName = 'HeartbeatResponseV1';
        } else {
            buffer = self.protocol.write().HeartbeatRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                generationId: generationId
            }).result;
            responseName = 'HeartbeatResponse';
        }

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer)[responseName]().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.syncConsumerGroupRequest = function (groupId, memberId, generationId, groupAssignment, protocolName) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var syncVersion = self._negotiateVersion(connection, globals.API_KEYS.SyncGroupRequest, 5);
        var buffer, responseName;

        if (syncVersion >= 5) {
            buffer = self.protocol.write().SyncConsumerGroupRequestV5({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                generationId: generationId,
                groupInstanceId: self.options.groupInstanceId || null,
                protocolType: 'consumer',
                protocolName: protocolName || null,
                groupAssignment: groupAssignment
            }).result;
            responseName = 'SyncConsumerGroupResponseV5';
        } else if (syncVersion >= 4) {
            buffer = self.protocol.write().SyncConsumerGroupRequestV4({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                generationId: generationId,
                groupInstanceId: self.options.groupInstanceId || null,
                groupAssignment: groupAssignment
            }).result;
            responseName = 'SyncConsumerGroupResponseV4';
        } else if (syncVersion >= 3) {
            buffer = self.protocol.write().SyncConsumerGroupRequestV3({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                generationId: generationId,
                groupInstanceId: self.options.groupInstanceId || null,
                groupAssignment: groupAssignment
            }).result;
            responseName = 'SyncConsumerGroupResponseV1'; // v3 response same as v1
        } else if (syncVersion >= 2) {
            buffer = self.protocol.write().SyncConsumerGroupRequestV2({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                generationId: generationId,
                groupAssignment: groupAssignment
            }).result;
            responseName = 'SyncConsumerGroupResponseV1'; // v2 response same as v1
        } else if (syncVersion >= 1) {
            buffer = self.protocol.write().SyncConsumerGroupRequestV1({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                generationId: generationId,
                groupAssignment: groupAssignment
            }).result;
            responseName = 'SyncConsumerGroupResponseV1';
        } else {
            buffer = self.protocol.write().SyncConsumerGroupRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                generationId: generationId,
                groupAssignment: groupAssignment
            }).result;
            responseName = 'SyncConsumerGroupResponse';
        }

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer)[responseName]().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.leaveGroupRequest = function (groupId, memberId) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var leaveVersion = self._negotiateVersion(connection, globals.API_KEYS.LeaveGroupRequest, 5);
        var buffer, responseName;

        if (leaveVersion >= 5) {
            buffer = self.protocol.write().LeaveGroupRequestV5({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                members: [{ memberId: memberId || '', groupInstanceId: self.options.groupInstanceId || null, reason: null }]
            }).result;
            responseName = 'LeaveGroupResponseV4';
        } else if (leaveVersion >= 4) {
            buffer = self.protocol.write().LeaveGroupRequestV4({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                members: [{ memberId: memberId || '', groupInstanceId: self.options.groupInstanceId || null }]
            }).result;
            responseName = 'LeaveGroupResponseV4';
        } else if (leaveVersion >= 3) {
            buffer = self.protocol.write().LeaveGroupRequestV3({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                members: [{ memberId: memberId || '', groupInstanceId: self.options.groupInstanceId || null }]
            }).result;
            responseName = 'LeaveGroupResponseV3';
        } else if (leaveVersion >= 2) {
            buffer = self.protocol.write().LeaveGroupRequestV2({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId
            }).result;
            responseName = 'LeaveGroupResponseV1'; // v2 response same as v1
        } else if (leaveVersion >= 1) {
            buffer = self.protocol.write().LeaveGroupRequestV1({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId
            }).result;
            responseName = 'LeaveGroupResponseV1';
        } else {
            buffer = self.protocol.write().LeaveGroupRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId
            }).result;
            responseName = 'LeaveGroupResponse';
        }

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer)[responseName]().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.offsetCommitRequestV2 = function (groupId, memberId, generationId, requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return self._findGroupCoordinator(groupId).then(function (connection) {
            var correlationId = self.nextCorrelationId();
            var commitVersion = self._negotiateVersion(connection, globals.API_KEYS.OffsetCommitRequest, 10);
            var buffer, responseName;

            if (commitVersion >= 10) {
                buffer = self.protocol.write().OffsetCommitRequestV10({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groupId: groupId,
                    generationId: generationId,
                    memberId: memberId,
                    groupInstanceId: self.options.groupInstanceId || null,
                    topics: requests.map(function (t) {
                        return {
                            topicId: self.topicIds[t.topicName] || null,
                            partitions: t.partitions
                        };
                    })
                }).result;
                responseName = 'OffsetCommitResponseV10';
            } else if (commitVersion >= 9) {
                buffer = self.protocol.write().OffsetCommitRequestV9({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groupId: groupId,
                    generationId: generationId,
                    memberId: memberId,
                    groupInstanceId: self.options.groupInstanceId || null,
                    topics: requests
                }).result;
                responseName = 'OffsetCommitResponseV8'; // v9 response same as v8
            } else if (commitVersion >= 8) {
                buffer = self.protocol.write().OffsetCommitRequestV8({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groupId: groupId,
                    generationId: generationId,
                    memberId: memberId,
                    groupInstanceId: self.options.groupInstanceId || null,
                    topics: requests
                }).result;
                responseName = 'OffsetCommitResponseV8';
            } else if (commitVersion >= 7) {
                buffer = self.protocol.write().OffsetCommitRequestV7({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groupId: groupId,
                    generationId: generationId,
                    memberId: memberId,
                    groupInstanceId: self.options.groupInstanceId || null,
                    topics: requests
                }).result;
                responseName = 'OffsetCommitResponseV6'; // v7 response same as v6
            } else if (commitVersion >= 6) {
                buffer = self.protocol.write().OffsetCommitRequestV6({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groupId: groupId,
                    generationId: generationId,
                    memberId: memberId,
                    topics: requests
                }).result;
                responseName = 'OffsetCommitResponseV6';
            } else if (commitVersion >= 3) {
                buffer = self.protocol.write().OffsetCommitRequestV3({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groupId: groupId,
                    generationId: generationId,
                    memberId: memberId,
                    retentionTime: self.options.retentionTime,
                    topics: requests
                }).result;
                responseName = 'OffsetCommitResponseV3';
            } else {
                buffer = self.protocol.write().OffsetCommitRequestV2({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    groupId: groupId,
                    generationId: generationId,
                    memberId: memberId,
                    retentionTime: self.options.retentionTime,
                    topics: requests
                }).result;
                responseName = 'OffsetCommitResponse';
            }

            return connection.send(correlationId, buffer).then(function (responseBuffer) {
                var topics = self.protocol.read(responseBuffer)[responseName]().result.topics;
                // v10 returns topicId instead of topicName — resolve from cache
                if (commitVersion >= 10) {
                    topics.forEach(function (t) {
                        t.topicName = self.topicNames[t.topicId] || t.topicId;
                    });
                }
                return topics;
            });
        })
        .then(_mapTopics);
    });
};

Client.prototype.listGroupsRequest = function () {
    var self = this;

    return self.updateMetadata().then(function () {
        return Promise.all(Object.values(self.brokerConnections).map(function (connection) {
            var correlationId = self.nextCorrelationId();
            var listVersion = self._negotiateVersion(connection, globals.API_KEYS.ListGroupsRequest, 5);
            var buffer, responseName;

            if (listVersion >= 5) {
                buffer = self.protocol.write().ListGroupsRequestV5({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    statesFilter: [],
                    typesFilter: []
                }).result;
                responseName = 'ListGroupResponseV5';
            } else if (listVersion >= 4) {
                buffer = self.protocol.write().ListGroupsRequestV4({
                    correlationId: correlationId,
                    clientId: self.options.clientId,
                    statesFilter: []
                }).result;
                responseName = 'ListGroupResponseV4';
            } else if (listVersion >= 3) {
                buffer = self.protocol.write().ListGroupsRequestV3({
                    correlationId: correlationId,
                    clientId: self.options.clientId
                }).result;
                responseName = 'ListGroupResponseV3';
            } else if (listVersion >= 2) {
                buffer = self.protocol.write().ListGroupsRequestV2({
                    correlationId: correlationId,
                    clientId: self.options.clientId
                }).result;
                responseName = 'ListGroupResponseV1'; // v2 response same as v1
            } else if (listVersion >= 1) {
                buffer = self.protocol.write().ListGroupsRequestV1({
                    correlationId: correlationId,
                    clientId: self.options.clientId
                }).result;
                responseName = 'ListGroupResponseV1';
            } else {
                buffer = self.protocol.write().ListGroupsRequest({
                    correlationId: correlationId,
                    clientId: self.options.clientId
                }).result;
                responseName = 'ListGroupResponse';
            }

            return connection.send(correlationId, buffer).then(function (responseBuffer) {
                return self.protocol.read(responseBuffer)[responseName]().result.groups;
            });
        }));
    }).then(function (r) { return [].concat.apply([], r); });
};

Client.prototype.describeGroupRequest = function (groupId) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var describeVersion = self._negotiateVersion(connection, globals.API_KEYS.DescribeGroupsRequest, 6);
        var buffer, responseName;

        if (describeVersion >= 6) {
            buffer = self.protocol.write().DescribeGroupRequestV6({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groups: [groupId]
            }).result;
            responseName = 'DescribeGroupResponseV6';
        } else if (describeVersion >= 5) {
            buffer = self.protocol.write().DescribeGroupRequestV5({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groups: [groupId]
            }).result;
            responseName = 'DescribeGroupResponseV5';
        } else if (describeVersion >= 4) {
            buffer = self.protocol.write().DescribeGroupRequestV4({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groups: [groupId]
            }).result;
            responseName = 'DescribeGroupResponseV4';
        } else if (describeVersion >= 3) {
            buffer = self.protocol.write().DescribeGroupRequestV3({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groups: [groupId]
            }).result;
            responseName = 'DescribeGroupResponseV3';
        } else if (describeVersion >= 2) {
            buffer = self.protocol.write().DescribeGroupRequestV2({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groups: [groupId]
            }).result;
            responseName = 'DescribeGroupResponseV1'; // v2 response same as v1
        } else if (describeVersion >= 1) {
            buffer = self.protocol.write().DescribeGroupRequestV1({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groups: [groupId]
            }).result;
            responseName = 'DescribeGroupResponseV1';
        } else {
            buffer = self.protocol.write().DescribeGroupRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groups: [groupId]
            }).result;
            responseName = 'DescribeGroupResponse';
        }

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            return self.protocol.read(responseBuffer)[responseName]().result.groups[0];
        });
    });
};

Client.prototype._decompressRecordBatches = function (batches) {
    var self = this;
    var decompress = self.options.asyncCompression ? compression.decompressAsync : compression.decompress;

    return Promise.all(batches.map(function (batch) {
        var header = batch.header;
        var codec = header.codec;
        var recordsRaw = batch.recordsRaw;

        var p = (codec > 0) ? decompress(recordsRaw, codec) : Promise.resolve(recordsRaw);

        return p.then(function (buf) {
            // parse individual Record entries
            var records = [], reader = self.protocol.read(buf), i, rec, timestampDelta;
            for (i = 0; i < header.recordCount; i++) {
                rec = reader.Record().result;
                timestampDelta = typeof rec.timestampDelta === 'object' ? rec.timestampDelta.toNumber() : rec.timestampDelta;
                records.push({
                    offset: header.baseOffset + rec.offsetDelta,
                    messageSize: rec.length,
                    message: {
                        key: rec.key,
                        value: rec.value,
                        timestamp: header.firstTimestamp + timestampDelta,
                        headers: rec.headers || [],
                        attributes: { codec: 0 }
                    }
                });
            }
            return records;
        })
        .catch(function (err) {
            self.error('Failed to decompress RecordBatch', err);
            return [];
        });
    })).then(function (r) { return [].concat.apply([], r); });
};

Client.prototype._decompressMessageSet = function (message) {
    var self = this;
    var decompress = self.options.asyncCompression ? compression.decompressAsync : compression.decompress;

    return decompress(message.value, message.attributes.codec).then(function (buffer) {
        return self.protocol.read(buffer).MessageSet(null, buffer.length).result;
    });
};

Client.prototype._compressMessageSet = function (messageSet, codec, compressionLevel) {
    var self = this, buffer;
    var compress = self.options.asyncCompression ? compression.compressAsync : compression.compress;

    if (codec === 0) { return Promise.resolve(messageSet); }
    buffer = self.protocol.write().MessageSet(messageSet).result;

    return compress(buffer, codec, compressionLevel).then(function (_buffer) {
        return [{
            offset: 0,
            message: {
                value: _buffer,
                attributes: {
                    codec: codec
                }
            }
        }];
    })
    .catch(function (err) {
        self.warn('Failed to compress messageSet', err);
        return messageSet;
    });
};

Client.prototype.apiVersionsRequest = function (connection) {
    var self = this, correlationId = self.nextCorrelationId();

    // Always send v0 request for bootstrap compatibility — broker responds with v0 format
    var buffer = self.protocol.write().ApiVersionsRequest({
        correlationId: correlationId,
        clientId: self.options.clientId
    }).result;

    return connection.send(correlationId, buffer).then(function (responseBuffer) {
        var result = self.protocol.read(responseBuffer).ApiVersionsResponse().result;
        var versionMap = {};
        if (!result.error && result.apiVersions) {
            result.apiVersions.forEach(function (v) {
                versionMap[v.apiKey] = { min: v.minVersion, max: v.maxVersion };
            });
        }
        connection.apiVersions = versionMap;
        return versionMap;
    })
    .catch(function (err) {
        if (err instanceof errors.NoKafkaConnectionError) {
            self.warn('apiVersionsRequest failed for', connection.server(), ':', err.message);
        }
        // old broker or transient failure — fall back to v0
        connection.apiVersions = {};
        return {};
    });
};

Client.prototype.initProducerIdRequest = function (transactionalId, transactionTimeout, connection) {
    var self = this, correlationId = self.nextCorrelationId();
    var initPidMax = 0, buffer;

    // check max InitProducerId version across initial brokers
    self.initialBrokers.forEach(function (conn) {
        if (conn.apiVersions && conn.apiVersions[globals.API_KEYS.InitProducerIdRequest]) {
            initPidMax = Math.max(initPidMax, conn.apiVersions[globals.API_KEYS.InitProducerIdRequest].max);
        }
    });

    if (initPidMax >= 6) {
        buffer = self.protocol.write().InitProducerIdRequestV6({
            correlationId: correlationId,
            clientId: self.options.clientId,
            transactionalId: transactionalId || null,
            transactionTimeoutMs: transactionTimeout || 0,
            producerId: self.producerId,
            producerEpoch: self.producerEpoch,
            enable2Pc: false,
            keepPreparedTxn: false
        }).result;
    } else if (initPidMax >= 3) {
        buffer = self.protocol.write().InitProducerIdRequestV3({
            correlationId: correlationId,
            clientId: self.options.clientId,
            apiVersion: Math.min(initPidMax, 5),
            transactionalId: transactionalId || null,
            transactionTimeoutMs: transactionTimeout || 0,
            producerId: self.producerId,
            producerEpoch: self.producerEpoch
        }).result;
    } else if (initPidMax >= 2) {
        buffer = self.protocol.write().InitProducerIdRequestV2({
            correlationId: correlationId,
            clientId: self.options.clientId,
            transactionalId: transactionalId || null,
            transactionTimeoutMs: transactionTimeout || 0
        }).result;
    } else if (initPidMax >= 1) {
        buffer = self.protocol.write().InitProducerIdRequestV1({
            correlationId: correlationId,
            clientId: self.options.clientId,
            transactionalId: transactionalId || null,
            transactionTimeoutMs: transactionTimeout || 0
        }).result;
    } else {
        buffer = self.protocol.write().InitProducerIdRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            transactionalId: transactionalId || null,
            transactionTimeoutMs: transactionTimeout || 0
        }).result;
    }

    function _parseResponse(responseBuffer) {
        var responseName = initPidMax >= 2 ? 'InitProducerIdResponseV2' : 'InitProducerIdResponse';
        var result = self.protocol.read(responseBuffer)[responseName]().result;
        if (result.error) {
            throw result.error;
        }
        // convert Int64BE to number
        if (typeof result.producerId === 'object' && result.producerId.toNumber) {
            result.producerId = result.producerId.toNumber();
        }
        return { producerId: result.producerId, producerEpoch: result.producerEpoch };
    }

    if (connection) {
        return connection.send(correlationId, buffer).then(_parseResponse);
    }

    return self._sendToAnyBroker(correlationId, buffer, _parseResponse);
};

Client.prototype._findTransactionCoordinator = function (transactionalId) {
    var self = this, buffer, correlationId = self.nextCorrelationId();
    var coordMax = 0, txnResponseName;

    if (self.transactionCoordinators[transactionalId]) {
        return self.transactionCoordinators[transactionalId];
    }

    // check max FindCoordinator version across initial brokers
    self.initialBrokers.forEach(function (conn) {
        if (conn.apiVersions && conn.apiVersions[globals.API_KEYS.GroupCoordinatorRequest]) {
            coordMax = Math.max(coordMax, conn.apiVersions[globals.API_KEYS.GroupCoordinatorRequest].max);
        }
    });
    if (coordMax >= 4) {
        buffer = self.protocol.write().FindCoordinatorRequestV4({
            correlationId: correlationId,
            clientId: self.options.clientId,
            apiVersion: Math.min(coordMax, 6),
            keyType: 1,
            coordinatorKeys: [transactionalId]
        }).result;
        txnResponseName = 'FindCoordinatorResponseV4';
    } else if (coordMax >= 3) {
        buffer = self.protocol.write().FindCoordinatorRequestV3({
            correlationId: correlationId,
            clientId: self.options.clientId,
            key: transactionalId,
            coordinatorType: 1
        }).result;
        txnResponseName = 'FindCoordinatorResponseV3';
    } else if (coordMax >= 2) {
        buffer = self.protocol.write().FindCoordinatorRequestV2({
            correlationId: correlationId,
            clientId: self.options.clientId,
            key: transactionalId,
            coordinatorType: 1
        }).result;
        txnResponseName = 'FindCoordinatorResponse';
    } else {
        buffer = self.protocol.write().FindCoordinatorRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            key: transactionalId,
            coordinatorType: 1
        }).result;
        txnResponseName = 'FindCoordinatorResponse';
    }

    self.transactionCoordinators[transactionalId] = self._sendToAnyBroker(correlationId, buffer, function (responseBuffer) {
        var result = self.protocol.read(responseBuffer)[txnResponseName]().result;
        var coord;
        if (result.coordinators) {
            coord = result.coordinators[0];
            if (coord.error) {
                throw coord.error;
            }
            return { coordinatorHost: coord.host, coordinatorPort: coord.port, coordinatorId: coord.nodeId };
        }
        if (result.error) {
            throw result.error;
        }
        return result;
    })
    .then(function (host) {
        var remapped = self.checkBrokerRedirect(host.coordinatorHost, host.coordinatorPort);
        var conn = self._createConnection(remapped.host, remapped.port);
        return self.apiVersionsRequest(conn).then(function () { return conn; });
    })
    .catch(function (err) {
        delete self.transactionCoordinators[transactionalId];
        throw err;
    });

    return self.transactionCoordinators[transactionalId];
};

Client.prototype.addPartitionsToTxnRequest = function (transactionalId, producerId, producerEpoch, topics) {
    var self = this;

    return self._findTransactionCoordinator(transactionalId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var txnVersion = self._negotiateVersion(connection, globals.API_KEYS.AddPartitionsToTxnRequest, 3);
        var buffer, responseName;

        if (txnVersion >= 3) {
            buffer = self.protocol.write().AddPartitionsToTxnRequestV3({
                correlationId: correlationId,
                clientId: self.options.clientId,
                transactionalId: transactionalId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                topics: topics
            }).result;
            responseName = 'AddPartitionsToTxnResponseV3';
        } else {
            buffer = self.protocol.write().AddPartitionsToTxnRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                apiVersion: txnVersion,
                transactionalId: transactionalId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                topics: topics
            }).result;
            responseName = 'AddPartitionsToTxnResponse';
        }

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer)[responseName]().result;
            // check for per-partition errors
            result.results.forEach(function (topicResult) {
                topicResult.partitions.forEach(function (p) {
                    if (p.error) {
                        throw p.error;
                    }
                });
            });
            return result;
        });
    })
    .catch(function (err) {
        if (err instanceof errors.NoKafkaConnectionError ||
            (err && (err.code === 'NotCoordinatorForGroup' || err.code === 'GroupCoordinatorNotAvailable'))) {
            delete self.transactionCoordinators[transactionalId];
        }
        throw err;
    });
};

Client.prototype.addOffsetsToTxnRequest = function (transactionalId, producerId, producerEpoch, groupId) {
    var self = this;

    return self._findTransactionCoordinator(transactionalId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var txnVersion = self._negotiateVersion(connection, globals.API_KEYS.AddOffsetsToTxnRequest, 4);
        var buffer, responseName;

        if (txnVersion >= 4) {
            buffer = self.protocol.write().AddOffsetsToTxnRequestV4({
                correlationId: correlationId,
                clientId: self.options.clientId,
                transactionalId: transactionalId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                groupId: groupId
            }).result;
            responseName = 'AddOffsetsToTxnResponseV3'; // v4 response same as v3
        } else if (txnVersion >= 3) {
            buffer = self.protocol.write().AddOffsetsToTxnRequestV3({
                correlationId: correlationId,
                clientId: self.options.clientId,
                transactionalId: transactionalId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                groupId: groupId
            }).result;
            responseName = 'AddOffsetsToTxnResponseV3';
        } else {
            buffer = self.protocol.write().AddOffsetsToTxnRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                apiVersion: txnVersion,
                transactionalId: transactionalId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                groupId: groupId
            }).result;
            responseName = 'AddOffsetsToTxnResponse';
        }

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer)[responseName]().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    })
    .catch(function (err) {
        if (err instanceof errors.NoKafkaConnectionError ||
            (err && (err.code === 'NotCoordinatorForGroup' || err.code === 'GroupCoordinatorNotAvailable'))) {
            delete self.transactionCoordinators[transactionalId];
        }
        throw err;
    });
};

Client.prototype.endTxnRequest = function (transactionalId, producerId, producerEpoch, committed) {
    var self = this;

    return self._findTransactionCoordinator(transactionalId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var txnVersion = self._negotiateVersion(connection, globals.API_KEYS.EndTxnRequest, 5);
        var buffer, responseName;

        if (txnVersion >= 4) {
            buffer = self.protocol.write().EndTxnRequestV4({
                correlationId: correlationId,
                clientId: self.options.clientId,
                apiVersion: txnVersion,
                transactionalId: transactionalId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                committed: committed
            }).result;
            responseName = txnVersion >= 5 ? 'EndTxnResponseV5' : 'EndTxnResponseV3';
        } else if (txnVersion >= 3) {
            buffer = self.protocol.write().EndTxnRequestV3({
                correlationId: correlationId,
                clientId: self.options.clientId,
                transactionalId: transactionalId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                committed: committed
            }).result;
            responseName = 'EndTxnResponseV3';
        } else {
            buffer = self.protocol.write().EndTxnRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                apiVersion: txnVersion,
                transactionalId: transactionalId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                committed: committed
            }).result;
            responseName = 'EndTxnResponse';
        }

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer)[responseName]().result;
            if (result.error) {
                throw result.error;
            }
            // EndTxn v5+: broker returns new producer epoch per transaction
            if (txnVersion >= 5 && result.producerEpoch !== undefined) {
                self.producerId = result.producerId;
                self.producerEpoch = result.producerEpoch;
            }
            return result;
        });
    })
    .catch(function (err) {
        if (err instanceof errors.NoKafkaConnectionError ||
            (err && (err.code === 'NotCoordinatorForGroup' || err.code === 'GroupCoordinatorNotAvailable'))) {
            delete self.transactionCoordinators[transactionalId];
        }
        throw err;
    });
};

Client.prototype.txnOffsetCommitRequest = function (transactionalId, groupId, producerId, producerEpoch, offsets) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        // group offsets by topic
        var topicGroups = utils.groupBy(offsets, function (o) { return o.topic; });
        var topics = Object.keys(topicGroups).map(function (topic) {
            return {
                topic: topic,
                partitions: topicGroups[topic].map(function (p) {
                    return { partition: p.partition, offset: p.offset, metadata: p.metadata || null };
                })
            };
        });

        var txnVersion = self._negotiateVersion(connection, globals.API_KEYS.TxnOffsetCommitRequest, 5);
        var buffer, responseName;

        if (txnVersion >= 4) {
            buffer = self.protocol.write().TxnOffsetCommitRequestV4({
                correlationId: correlationId,
                clientId: self.options.clientId,
                apiVersion: txnVersion,
                transactionalId: transactionalId,
                groupId: groupId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                generationId: -1,
                memberId: '',
                groupInstanceId: null,
                topics: topics
            }).result;
            responseName = 'TxnOffsetCommitResponseV3'; // v4/v5 response same as v3
        } else if (txnVersion >= 3) {
            buffer = self.protocol.write().TxnOffsetCommitRequestV3({
                correlationId: correlationId,
                clientId: self.options.clientId,
                transactionalId: transactionalId,
                groupId: groupId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                generationId: -1,
                memberId: '',
                groupInstanceId: null,
                topics: topics
            }).result;
            responseName = 'TxnOffsetCommitResponseV3';
        } else if (txnVersion >= 2) {
            buffer = self.protocol.write().TxnOffsetCommitRequestV2({
                correlationId: correlationId,
                clientId: self.options.clientId,
                apiVersion: txnVersion,
                transactionalId: transactionalId,
                groupId: groupId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                topics: topics
            }).result;
            responseName = 'TxnOffsetCommitResponse';
        } else {
            buffer = self.protocol.write().TxnOffsetCommitRequest({
                correlationId: correlationId,
                clientId: self.options.clientId,
                apiVersion: txnVersion,
                transactionalId: transactionalId,
                groupId: groupId,
                producerId: producerId,
                producerEpoch: producerEpoch,
                topics: topics
            }).result;
            responseName = 'TxnOffsetCommitResponse';
        }

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer)[responseName]().result;
            // check for per-partition errors
            result.results.forEach(function (topicResult) {
                topicResult.partitions.forEach(function (p) {
                    if (p.error) {
                        throw p.error;
                    }
                });
            });
            return result;
        });
    });
};

Client.prototype._applyLeaderHints = function (topics, nodeEndpoints) {
    var self = this;

    // Create broker connections from nodeEndpoints if missing
    if (nodeEndpoints && nodeEndpoints.length) {
        nodeEndpoints.forEach(function (ep) {
            var remapped;
            if (!self.brokerConnections[ep.nodeId]) {
                remapped = self.checkBrokerRedirect(ep.host, ep.port);
                self.brokerConnections[ep.nodeId] = self._createConnection(remapped.host, remapped.port);
                if (ep.rack) {
                    self.brokerRacks[ep.nodeId] = ep.rack;
                }
                Promise.resolve(self.apiVersionsRequest(self.brokerConnections[ep.nodeId])).catch(function (err) {
                    self.warn('Failed to discover API versions for node', ep.nodeId, err);
                });
            }
        });
    }

    // Update topicMetadata leader from currentLeader hints
    topics.forEach(function (t) {
        var topicName = t.topicName;
        if (!topicName || !self.topicMetadata[topicName]) {
            return;
        }
        t.partitions.forEach(function (p) {
            if (p.currentLeader && p.currentLeader.leaderId >= 0 &&
                self.topicMetadata[topicName][p.partition]) {
                self.topicMetadata[topicName][p.partition].leader = p.currentLeader.leaderId;
            }
        });
    });
};

// ============================
// KIP-932: Share Groups
// ============================

Client.prototype.updateShareCoordinator = function (groupId) {
    var self = this;
    if (self.shareCoordinators[groupId]) {
        return self.shareCoordinators[groupId].then(function (connection) {
            connection.close();
            delete self.shareCoordinators[groupId];
        })
        .catch(function (err) {
            self.warn('Failed to close share coordinator for', groupId, err);
            delete self.shareCoordinators[groupId];
        });
    }
    delete self.shareCoordinators[groupId];
    return Promise.resolve();
};

Client.prototype._findShareCoordinator = function (groupId) {
    var self = this, buffer, correlationId = self.nextCorrelationId();
    var coordMax = 0, responseName;

    if (self.shareCoordinators[groupId]) {
        return self.shareCoordinators[groupId];
    }

    // check max FindCoordinator version across initial brokers
    self.initialBrokers.forEach(function (conn) {
        if (conn.apiVersions && conn.apiVersions[globals.API_KEYS.GroupCoordinatorRequest]) {
            coordMax = Math.max(coordMax, conn.apiVersions[globals.API_KEYS.GroupCoordinatorRequest].max);
        }
    });

    // Share coordinators require FindCoordinator v4+ with keyType=2
    if (coordMax >= 4) {
        buffer = self.protocol.write().FindCoordinatorRequestV4({
            correlationId: correlationId,
            clientId: self.options.clientId,
            apiVersion: Math.min(coordMax, 6),
            keyType: 2,
            coordinatorKeys: [groupId]
        }).result;
        responseName = 'FindCoordinatorResponseV4';
    } else if (coordMax >= 3) {
        buffer = self.protocol.write().FindCoordinatorRequestV3({
            correlationId: correlationId,
            clientId: self.options.clientId,
            key: groupId,
            coordinatorType: 2
        }).result;
        responseName = 'FindCoordinatorResponseV3';
    } else {
        return Promise.reject(new Error('Share groups require FindCoordinator v3+ (keyType=2)'));
    }

    self.shareCoordinators[groupId] = self._sendToAnyBroker(correlationId, buffer, function (responseBuffer) {
        var result = self.protocol.read(responseBuffer)[responseName]().result;
        var coord;
        if (result.coordinators) {
            coord = result.coordinators[0];
            if (coord.error) {
                throw coord.error;
            }
            return { coordinatorHost: coord.host, coordinatorPort: coord.port, coordinatorId: coord.nodeId };
        }
        if (result.error) {
            throw result.error;
        }
        return result;
    })
    .then(function (host) {
        var remapped = self.checkBrokerRedirect(host.coordinatorHost, host.coordinatorPort);
        var conn = self._createConnection(remapped.host, remapped.port);
        return self.apiVersionsRequest(conn).then(function () { return conn; });
    })
    .catch(function (err) {
        delete self.shareCoordinators[groupId];
        throw err;
    });

    return self.shareCoordinators[groupId];
};

Client.prototype.shareGroupHeartbeatRequest = function (groupId, memberId, memberEpoch, rackId, subscribedTopicNames) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var buffer = self.protocol.write().ShareGroupHeartbeatRequestV1({
            correlationId: correlationId,
            clientId: self.options.clientId,
            groupId: groupId,
            memberId: memberId || '',
            memberEpoch: memberEpoch,
            rackId: rackId || null,
            subscribedTopicNames: subscribedTopicNames || null
        }).result;

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer).ShareGroupHeartbeatResponseV1().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.shareFetchRequest = function (groupId, memberId, shareSessionEpoch, requests, forgottenTopics, acknowledgements) {
    var self = this;

    return self._waitMetadata().then(function () {
        return Promise.all(Object.keys(requests).map(function (leader) {
            var topics = requests[leader];
            var correlationId = self.nextCorrelationId();
            var conn, buffer;

            if (leader === -1 || !self.brokerConnections[leader]) {
                return _fakeTopicsErrorResponse(topics, errors.byName('LeaderNotAvailable'));
            }

            conn = self.brokerConnections[leader];

            buffer = self.protocol.write().ShareFetchRequestV1({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                shareSessionEpoch: shareSessionEpoch,
                maxWaitMs: self.options.maxWaitTime,
                minBytes: self.options.minBytes,
                maxBytes: self.options.maxBytes || 0x7fffffff,
                topics: topics.map(function (t) {
                    var acks = acknowledgements && acknowledgements[t.topicName];
                    return {
                        topicId: self.topicIds[t.topicName] || null,
                        partitions: t.partitions.map(function (p) {
                            var partAcks = acks && acks[p.partition];
                            return {
                                partitionIndex: p.partition,
                                acknowledgementBatches: partAcks || []
                            };
                        })
                    };
                }),
                forgottenTopicsData: forgottenTopics || []
            }).result;

            return conn.send(correlationId, buffer).then(function (responseBuffer) {
                var parsed = self.protocol.read(responseBuffer).ShareFetchResponseV1().result;
                // Resolve topicId back to topicName
                (parsed.responses || []).forEach(function (t) {
                    t.topicName = self.topicNames[t.topicId] || t.topicId;
                });
                return parsed.responses || [];
            })
            .catch(function (err) {
                if (err instanceof errors.NoKafkaConnectionError) {
                    return _fakeTopicsErrorResponse(topics, err);
                }
                throw err;
            });
        }));
    })
    .then(function (results) {
        // Flatten results: array of topic arrays → flat array of {topic, partition, ...} items
        var flat = [], i, j, k, topicArr, t, p;
        for (i = 0; i < results.length; i++) {
            topicArr = results[i];
            if (!Array.isArray(topicArr)) { continue; }
            for (j = 0; j < topicArr.length; j++) {
                t = topicArr[j];
                for (k = 0; k < (t.partitions || []).length; k++) {
                    p = t.partitions[k];
                    flat.push({
                        topic: t.topicName,
                        topicId: t.topicId,
                        partition: p.partitionIndex,
                        error: p.error,
                        errorMessage: p.errorMessage,
                        acknowledgeError: p.acknowledgeError,
                        acknowledgeErrorMessage: p.acknowledgeErrorMessage,
                        currentLeader: p.currentLeader,
                        messageSet: p.messageSet || [],
                        acquiredRecords: p.acquiredRecords || []
                    });
                }
            }
        }
        return self._decompressShareFetchResults(flat);
    });
};

// Decompress RecordBatch entries in share fetch results (same logic as regular fetch)
Client.prototype._decompressShareFetchResults = function (results) {
    var self = this;
    return Promise.all(results.map(function (r) {
        var messageSet = r.messageSet;
        if (messageSet.length > 0 && messageSet[0].header && messageSet[0].recordsRaw) {
            return self._decompressRecordBatches(messageSet).then(function (converted) {
                return Object.assign({}, r, { messageSet: converted });
            });
        }
        return r;
    }));
};

Client.prototype.shareAcknowledgeRequest = function (groupId, memberId, shareSessionEpoch, requests) {
    var self = this;

    return self._waitMetadata().then(function () {
        return Promise.all(Object.keys(requests).map(function (leader) {
            var topics = requests[leader];
            var correlationId = self.nextCorrelationId();
            var conn, buffer;

            if (leader === -1 || !self.brokerConnections[leader]) {
                return [];
            }

            conn = self.brokerConnections[leader];

            buffer = self.protocol.write().ShareAcknowledgeRequestV1({
                correlationId: correlationId,
                clientId: self.options.clientId,
                groupId: groupId,
                memberId: memberId,
                shareSessionEpoch: shareSessionEpoch,
                topics: topics.map(function (t) {
                    return {
                        topicId: self.topicIds[t.topicName] || null,
                        partitions: t.partitions
                    };
                })
            }).result;

            return conn.send(correlationId, buffer).then(function (responseBuffer) {
                var parsed = self.protocol.read(responseBuffer).ShareAcknowledgeResponseV1().result;
                if (parsed.error) {
                    throw parsed.error;
                }
                // Resolve topicId back to topicName
                (parsed.responses || []).forEach(function (t) {
                    t.topicName = self.topicNames[t.topicId] || t.topicId;
                });
                return parsed.responses || [];
            });
        }));
    })
    .then(function (results) {
        // Flatten to [{topic, partition, error, currentLeader}]
        var flat = [], i, j, k, topicArr, t, p;
        for (i = 0; i < results.length; i++) {
            topicArr = results[i];
            if (!Array.isArray(topicArr)) { continue; }
            for (j = 0; j < topicArr.length; j++) {
                t = topicArr[j];
                for (k = 0; k < (t.partitions || []).length; k++) {
                    p = t.partitions[k];
                    flat.push({
                        topic: t.topicName,
                        partition: p.partitionIndex,
                        error: p.error,
                        currentLeader: p.currentLeader
                    });
                }
            }
        }
        return flat;
    });
};

Client.prototype._negotiateVersion = function (connection, apiKey, clientMax) {
    var versions = connection.apiVersions;
    if (!versions || !versions[apiKey]) {
        return 0;
    }
    return Math.min(clientMax, versions[apiKey].max);
};

Client.prototype.validateId = function (id) {
    return id.search(/[?:,"]/) === -1;
};
