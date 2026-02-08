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
