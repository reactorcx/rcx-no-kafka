'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

/////////////////////////////
// INIT PRODUCER ID API    //
/////////////////////////////

Protocol.define('InitProducerIdRequest', {
    write: function (data) { // { transactionalId, transactionTimeoutMs }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.InitProducerIdRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.transactionalId || null)
            .Int32BE(data.transactionTimeoutMs || 0);
    }
});

Protocol.define('InitProducerIdResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .Int64BE('producerId')
            .Int16BE('producerEpoch');
    }
});

// v1 — same wire format as v0
// v2 — first flexible version (KIP-482)
Protocol.define('InitProducerIdRequestV2', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.InitProducerIdRequest,
                apiVersion: 2,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactNullableString(data.transactionalId || null)
            .Int32BE(data.transactionTimeoutMs || 0)
            .TaggedFields();
    }
});

Protocol.define('InitProducerIdResponseV2', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .Int64BE('producerId')
            .Int16BE('producerEpoch')
            .TaggedFields();
    }
});

// v3 (KIP-588) — adds producerId/producerEpoch to request for epoch recovery
// v4 (KIP-890) and v5 (KIP-890) same wire format, reuse via variable apiVersion
Protocol.define('InitProducerIdRequestV3', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.InitProducerIdRequest,
                apiVersion: data.apiVersion || 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactNullableString(data.transactionalId || null)
            .Int32BE(data.transactionTimeoutMs || 0)
            .Int64BE(data.producerId !== undefined ? data.producerId : -1)
            .Int16BE(data.producerEpoch !== undefined ? data.producerEpoch : -1)
            .TaggedFields();
    }
});

// v6 (KIP-939) — adds enable2Pc and keepPreparedTxn to request
Protocol.define('InitProducerIdRequestV6', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.InitProducerIdRequest,
                apiVersion: 6,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactNullableString(data.transactionalId || null)
            .Int32BE(data.transactionTimeoutMs || 0)
            .Int64BE(data.producerId !== undefined ? data.producerId : -1)
            .Int16BE(data.producerEpoch !== undefined ? data.producerEpoch : -1)
            .Int8(data.enable2Pc ? 1 : 0)
            .Int8(data.keepPreparedTxn ? 1 : 0)
            .TaggedFields();
    }
});

Protocol.define('InitProducerIdRequestV1', {
    write: function (data) { // { transactionalId, transactionTimeoutMs }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.InitProducerIdRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.transactionalId || null)
            .Int32BE(data.transactionTimeoutMs || 0);
    }
});
