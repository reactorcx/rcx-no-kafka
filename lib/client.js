'use strict';

/* global AggregateError */

var Connection    = require('./connection');
var Protocol      = require('./protocol');
var errors        = require('./errors');
var _             = require('lodash');
var Logger        = require('nice-simple-logger');
var compression   = require('./protocol/misc/compression');
var globals       = require('./protocol/globals');
var url           = require('url');
var fs            = require('fs');
var util          = require('util');
var Kafka         = require('./index');
var promiseUtils  = require('./promise-utils');

function Client(options) {
    var self = this, logger;

    self.options = _.defaultsDeep(options || {}, {
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
        self[m] = _.bind(logger[m], logger, self.options.clientId);
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

    self._updateMetadata_running = Promise.resolve();
    self._updateMetadata_pending = false;
}

module.exports = Client;

function _mapTopics(topics) {
    return _(topics).flatten().transform(function (a, tv) {
        if (tv === null) { return; } // requiredAcks=0
        _.each(tv.partitions, function (p) {
            a.push(_.merge({
                topic: tv.topicName,
                partition: p.partition
            },
             _.omit(p, 'partition')
            /*function (_a, b) {if (b instanceof Buffer) {return b;}}*/) // fix for lodash _merge in Node v4: https://github.com/lodash/lodash/issues/1453
            );
        });
        return;
    }, []).value();
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

        self.initialBrokers = _.compact(self.initialBrokers);

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

Client.prototype.end = function () {
    var self = this;

    self.finished = true;

    return Promise.all(
        Array.prototype.concat(self.initialBrokers, _.values(self.brokerConnections), _.values(self.groupCoordinators), _.values(self.transactionCoordinators))
        .map(function (c) {
            if (c && typeof c.close === 'function') {
                return c.close();
            }
            // handle Promise-wrapped connections
            if (c && typeof c.then === 'function') {
                return c.then(function (conn) { return conn.close(); }).catch(function () {});
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

        return self.metadataRequest(topicNames).then(function (response) {
            var oldConnections = self.brokerConnections;

            self.brokerConnections = {};

            _.each(response.broker, function (broker) {
                var remapped = self.checkBrokerRedirect(broker.host, broker.port);
                var deleteKey = undefined;
                var connection = _.find(oldConnections, function (c, key) {
                    if (c.equal(remapped.host, remapped.port)) {
                        deleteKey = key;
                        return true;
                    }
                    return false;
                });
                if (deleteKey !== undefined) {
                    delete oldConnections[deleteKey];
                }

                self.brokerConnections[broker.nodeId] = connection || self._createConnection(remapped.host, remapped.port);
                if (broker.rack) {
                    self.brokerRacks[broker.nodeId] = broker.rack;
                }
            });

            _.each(oldConnections, function (c) {c.close();});

            _.each(response.topicMetadata, function (topic) {
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

            if (_.isEmpty(self.brokerConnections)) {
                self.warn('No broker metadata received, retrying metadata request in 1000ms');
                return promiseUtils.delay(1000).then(_try);
            }

            // discover API versions for each new broker connection
            return Promise.all(_.map(self.brokerConnections, function (conn) {
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

Client.prototype.metadataRequest = function (topicNames) {
    var self = this, correlationId = self.nextCorrelationId();
    var metadataMax = 0;
    var buffer, responseName;

    // check max metadata version supported by any initial broker
    _.each(self.initialBrokers, function (conn) {
        if (conn.apiVersions && conn.apiVersions[globals.API_KEYS.MetadataRequest]) {
            metadataMax = Math.max(metadataMax, conn.apiVersions[globals.API_KEYS.MetadataRequest].max);
        }
    });
    if (metadataMax >= 10) {
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

    return Promise.any(self.initialBrokers.map(function (connection) {
        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer)[responseName]().result;
            if (result.controllerId !== undefined) {
                self.controllerId = result.controllerId;
            }
            if (result.clusterId !== undefined) {
                self.clusterId = result.clusterId;
            }
            return result;
        });
    }))
    .catch(function (err) {
        var errs = err instanceof AggregateError ? err.errors : [err];
        if (errs.length === 1) {
            throw errs[0];
        }
        throw errs;
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
    .then(_.values);
};

Client.prototype.findLeader = function (topic, partition) {
    var self = this;

    function _try() {
        var r = _.get(self.topicMetadata, [topic, partition, 'leader'], -1);
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
    return _.result(this.brokerConnections, [leader, 'server'], '-');
};

function _fakeTopicsErrorResponse(topics, error) {
    return _.map(topics, function (t) {
        return {
            topicName: t.topicName,
            partitions: _.map(t.partitions, function (p) {
                return {
                    partition: p.partition,
                    error: error
                };
            })
        };
    });
}

Client.prototype.produceRequest = function (requests, codec) {
    var self = this, compressionPromises = [];

    var compress = self.options.asyncCompression ? compression.compressAsync : compression.compress;

    function processPartition(pv, pk, leader) {
        var conn = self.brokerConnections[leader];
        var produceVersion = conn ? self._negotiateVersion(conn, globals.API_KEYS.ProduceRequest, 9) : 0;
        var partition = parseInt(pk);
        var _r, recordsList, firstTs, maxTs, j, recTs, tmpWriter, recordsBuf;

        if (produceVersion >= 3) {
            // RecordBatch v2 format
            recordsList = _.map(pv, function (mv) {
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

                compressionPromises.push(compress(recordsBuf, codec).then(function (compressed) {
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
        compressionPromises.push(self._compressMessageSet(_.map(pv, function (mv) {
            return { offset: 0, message: mv.message };
        }), codec).then(function (messageSet) {
            _r.messageSet = messageSet;
        }));
        return _r;
    }

    return self._waitMetadata().then(function () {
        requests = _(requests).groupBy('leader').mapValues(function (v, leader) {
            return _(v)
                .groupBy('topic')
                .map(function (p, t) {
                    return {
                        topicName: t,
                        partitions: _(p).groupBy('partition').map(function (pv, pk) {
                            return processPartition(pv, pk, leader);
                        }).value()
                    };
                })
                .value();
        }).value();

        return Promise.all(compressionPromises).then(function () {
            return Promise.all(_.map(requests, function (topics, leader) {
                var correlationId = self.nextCorrelationId();
                var conn = self.brokerConnections[leader];
                var produceVersion = conn ? self._negotiateVersion(conn, globals.API_KEYS.ProduceRequest, 9) : 0;
                var buffer, responseName;

                if (produceVersion >= 9) {
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
                    if (self.options.requiredAcks !== 0) {
                        return self.protocol.read(responseBuffer)[responseName]().result.topics;
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
        return Promise.all(_.map(requests, function (topics, leader) {
            var buffer, correlationId = self.nextCorrelationId(), conn, fetchVersion, responseName;
            // fake LeaderNotAvailable for all topics with no leader
            if (leader === -1 || !self.brokerConnections[leader]) {
                return _fakeTopicsErrorResponse(topics, errors.byName('LeaderNotAvailable'));
            }

            conn = self.brokerConnections[leader];
            fetchVersion = self._negotiateVersion(conn, globals.API_KEYS.FetchRequest, 13);

            if (fetchVersion >= 13) {
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
                var result = self.protocol.read(responseBuffer)[responseName]().result.topics;
                // KIP-516: resolve topicId back to topicName for v13 responses
                if (responseName === 'FetchResponseV13') {
                    _.each(result, function (t) {
                        t.topicName = self.topicNames[t.topicId] || t.topicId;
                    });
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
                        return _.assign({}, r, { messageSet: converted });
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
                    return _.assign({}, r, { messageSet: _.flatten(newMessageSet) });
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
        return Promise.all(_.map(requests, function (topics, leader) {
            var correlationId = self.nextCorrelationId();
            var conn = self.brokerConnections[leader];
            var offsetVersion = conn ? self._negotiateVersion(conn, globals.API_KEYS.OffsetRequest, 6) : 0;
            var buffer, responseName;

            if (offsetVersion >= 6) {
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
        return Promise.all(_.map(requests, function (topics, leader) {
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
        return Promise.all(_.map(requests, function (topics, leader) {
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
            var fetchVersion = self._negotiateVersion(connection, globals.API_KEYS.OffsetFetchRequest, 6);
            var buffer, responseName;

            if (fetchVersion >= 6) {
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
        .catch(function () {
            delete self.groupCoordinators[groupId];
        });
    }
    delete self.groupCoordinators[groupId];
    return Promise.resolve();
};

Client.prototype._findGroupCoordinator = function (groupId) {
    var self = this, buffer, correlationId = self.nextCorrelationId();
    var coordMax = 0, responseName;

    if (self.groupCoordinators[groupId]) {
        return self.groupCoordinators[groupId];
    }

    // check max FindCoordinator version across initial brokers
    _.each(self.initialBrokers, function (conn) {
        if (conn.apiVersions && conn.apiVersions[globals.API_KEYS.GroupCoordinatorRequest]) {
            coordMax = Math.max(coordMax, conn.apiVersions[globals.API_KEYS.GroupCoordinatorRequest].max);
        }
    });

    if (coordMax >= 3) {
        buffer = self.protocol.write().FindCoordinatorRequestV3({
            correlationId: correlationId,
            clientId: self.options.clientId,
            key: groupId,
            coordinatorType: 0
        }).result;
        responseName = 'FindCoordinatorResponseV3';
    } else if (coordMax >= 1) {
        buffer = self.protocol.write().FindCoordinatorRequestV2({
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

    self.groupCoordinators[groupId] = Promise.any(self.initialBrokers.map(function (connection) {
        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer)[responseName]().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    }))
    .then(function (host) {
        var remapped = self.checkBrokerRedirect(host.coordinatorHost, host.coordinatorPort);
        var conn = self._createConnection(remapped.host, remapped.port);
        return self.apiVersionsRequest(conn).then(function () { return conn; });
    })
    .catch(function (err) {
        var errs = err instanceof AggregateError ? err.errors : [err];
        delete self.groupCoordinators[groupId];
        if (errs.length === 1) {
            throw errs[0];
        }
        throw errs;
    });

    return self.groupCoordinators[groupId];
};

Client.prototype.joinConsumerGroupRequest = function (groupId, memberId, sessionTimeout, strategies, rebalanceTimeout) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var joinVersion = self._negotiateVersion(connection, globals.API_KEYS.JoinGroupRequest, 6);
        var buffer, responseName;

        if (joinVersion >= 6) {
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

Client.prototype.syncConsumerGroupRequest = function (groupId, memberId, generationId, groupAssignment) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var syncVersion = self._negotiateVersion(connection, globals.API_KEYS.SyncGroupRequest, 4);
        var buffer, responseName;

        if (syncVersion >= 4) {
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
        var leaveVersion = self._negotiateVersion(connection, globals.API_KEYS.LeaveGroupRequest, 4);
        var buffer, responseName;

        if (leaveVersion >= 4) {
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
            var commitVersion = self._negotiateVersion(connection, globals.API_KEYS.OffsetCommitRequest, 8);
            var buffer, responseName;

            if (commitVersion >= 8) {
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
                return self.protocol.read(responseBuffer)[responseName]().result.topics;
            });
        })
        .then(_mapTopics);
    });
};

Client.prototype.listGroupsRequest = function () {
    var self = this;

    return self.updateMetadata().then(function () {
        return Promise.all(_.values(self.brokerConnections).map(function (connection) {
            var correlationId = self.nextCorrelationId();
            var listVersion = self._negotiateVersion(connection, globals.API_KEYS.ListGroupsRequest, 3);
            var buffer, responseName;

            if (listVersion >= 3) {
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
    }).then(_.flatten);
};

Client.prototype.describeGroupRequest = function (groupId) {
    var self = this;

    return self._findGroupCoordinator(groupId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var describeVersion = self._negotiateVersion(connection, globals.API_KEYS.DescribeGroupsRequest, 5);
        var buffer, responseName;

        if (describeVersion >= 5) {
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
    })).then(_.flatten);
};

Client.prototype._decompressMessageSet = function (message) {
    var self = this;
    var decompress = self.options.asyncCompression ? compression.decompressAsync : compression.decompress;

    return decompress(message.value, message.attributes.codec).then(function (buffer) {
        return self.protocol.read(buffer).MessageSet(null, buffer.length).result;
    });
};

Client.prototype._compressMessageSet = function (messageSet, codec) {
    var self = this, buffer;
    var compress = self.options.asyncCompression ? compression.compressAsync : compression.compress;

    if (codec === 0) { return Promise.resolve(messageSet); }
    buffer = self.protocol.write().MessageSet(messageSet).result;

    return compress(buffer, codec).then(function (_buffer) {
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
    .catch(function () {
        // old broker that doesn't support ApiVersions — fall back to v0
        connection.apiVersions = {};
        return {};
    });
};

Client.prototype.initProducerIdRequest = function (transactionalId, transactionTimeout, connection) {
    var self = this, correlationId = self.nextCorrelationId();
    var initPidMax = 0, buffer;

    // check max InitProducerId version across initial brokers
    _.each(self.initialBrokers, function (conn) {
        if (conn.apiVersions && conn.apiVersions[globals.API_KEYS.InitProducerIdRequest]) {
            initPidMax = Math.max(initPidMax, conn.apiVersions[globals.API_KEYS.InitProducerIdRequest].max);
        }
    });

    if (initPidMax >= 2) {
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

    function _sendAndParse(conn) {
        return conn.send(correlationId, buffer).then(function (responseBuffer) {
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
        });
    }

    if (connection) {
        return _sendAndParse(connection);
    }

    return Promise.any(self.initialBrokers.map(function (conn) {
        return _sendAndParse(conn);
    }))
    .catch(function (err) {
        var errs = err instanceof AggregateError ? err.errors : [err];
        if (errs.length === 1) {
            throw errs[0];
        }
        throw errs;
    });
};

Client.prototype._findTransactionCoordinator = function (transactionalId) {
    var self = this, buffer, correlationId = self.nextCorrelationId();
    var coordMax = 0, txnResponseName;

    if (self.transactionCoordinators[transactionalId]) {
        return self.transactionCoordinators[transactionalId];
    }

    // check max FindCoordinator version across initial brokers
    _.each(self.initialBrokers, function (conn) {
        if (conn.apiVersions && conn.apiVersions[globals.API_KEYS.GroupCoordinatorRequest]) {
            coordMax = Math.max(coordMax, conn.apiVersions[globals.API_KEYS.GroupCoordinatorRequest].max);
        }
    });
    if (coordMax >= 3) {
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

    self.transactionCoordinators[transactionalId] = Promise.any(self.initialBrokers.map(function (connection) {
        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer)[txnResponseName]().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    }))
    .then(function (host) {
        var remapped = self.checkBrokerRedirect(host.coordinatorHost, host.coordinatorPort);
        var conn = self._createConnection(remapped.host, remapped.port);
        return self.apiVersionsRequest(conn).then(function () { return conn; });
    })
    .catch(function (err) {
        var errs = err instanceof AggregateError ? err.errors : [err];
        delete self.transactionCoordinators[transactionalId];
        if (errs.length === 1) {
            throw errs[0];
        }
        throw errs;
    });

    return self.transactionCoordinators[transactionalId];
};

Client.prototype.addPartitionsToTxnRequest = function (transactionalId, producerId, producerEpoch, topics) {
    var self = this;

    return self._findTransactionCoordinator(transactionalId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var buffer = self.protocol.write().AddPartitionsToTxnRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            transactionalId: transactionalId,
            producerId: producerId,
            producerEpoch: producerEpoch,
            topics: topics
        }).result;

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer).AddPartitionsToTxnResponse().result;
            // check for per-partition errors
            _.each(result.results, function (topicResult) {
                _.each(topicResult.partitions, function (p) {
                    if (p.error) {
                        throw p.error;
                    }
                });
            });
            return result;
        });
    });
};

Client.prototype.addOffsetsToTxnRequest = function (transactionalId, producerId, producerEpoch, groupId) {
    var self = this;

    return self._findTransactionCoordinator(transactionalId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var buffer = self.protocol.write().AddOffsetsToTxnRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            transactionalId: transactionalId,
            producerId: producerId,
            producerEpoch: producerEpoch,
            groupId: groupId
        }).result;

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer).AddOffsetsToTxnResponse().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.endTxnRequest = function (transactionalId, producerId, producerEpoch, committed) {
    var self = this;

    return self._findTransactionCoordinator(transactionalId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        var buffer = self.protocol.write().EndTxnRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            transactionalId: transactionalId,
            producerId: producerId,
            producerEpoch: producerEpoch,
            committed: committed
        }).result;

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer).EndTxnResponse().result;
            if (result.error) {
                throw result.error;
            }
            return result;
        });
    });
};

Client.prototype.txnOffsetCommitRequest = function (transactionalId, groupId, producerId, producerEpoch, offsets) {
    var self = this;

    return self._findTransactionCoordinator(transactionalId).then(function (connection) {
        var correlationId = self.nextCorrelationId();
        // group offsets by topic
        var topics = _(offsets).groupBy('topic').map(function (parts, topic) {
            return {
                topic: topic,
                partitions: _.map(parts, function (p) {
                    return { partition: p.partition, offset: p.offset, metadata: p.metadata || null };
                })
            };
        }).value();

        var buffer = self.protocol.write().TxnOffsetCommitRequest({
            correlationId: correlationId,
            clientId: self.options.clientId,
            transactionalId: transactionalId,
            groupId: groupId,
            producerId: producerId,
            producerEpoch: producerEpoch,
            topics: topics
        }).result;

        return connection.send(correlationId, buffer).then(function (responseBuffer) {
            var result = self.protocol.read(responseBuffer).TxnOffsetCommitResponse().result;
            // check for per-partition errors
            _.each(result.results, function (topicResult) {
                _.each(topicResult.partitions, function (p) {
                    if (p.error) {
                        throw p.error;
                    }
                });
            });
            return result;
        });
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
