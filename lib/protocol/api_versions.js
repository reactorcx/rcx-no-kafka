'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

//////////////////////
// API VERSIONS API //
//////////////////////

Protocol.define('ApiVersionsRequest', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ApiVersionsRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            });
    }
});

Protocol.define('ApiVersionsResponseItem', {
    read: function () {
        this
            .Int16BE('apiKey')
            .Int16BE('minVersion')
            .Int16BE('maxVersion');
    }
});

Protocol.define('ApiVersionsResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .array('apiVersions', this.ApiVersionsResponseItem);
    }
});
