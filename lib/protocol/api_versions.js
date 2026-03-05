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

// v1 — same request wire format (keep sending v0 for bootstrap), response adds throttleTime
Protocol.define('ApiVersionsRequestV1', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ApiVersionsRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            });
    }
});

Protocol.define('ApiVersionsResponseV1', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .array('apiVersions', this.ApiVersionsResponseItem)
            .Int32BE('throttleTime');
    }
});

// v2 — same wire format as v1
Protocol.define('ApiVersionsRequestV2', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ApiVersionsRequest,
                apiVersion: 2,
                correlationId: data.correlationId,
                clientId: data.clientId
            });
    }
});

// v3 — first flexible version (KIP-482), adds client software name/version
Protocol.define('ApiVersionsRequestV3', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ApiVersionsRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.clientSoftwareName || '')
            .compactString(data.clientSoftwareVersion || '')
            .TaggedFields();
    }
});

Protocol.define('ApiVersionsResponseV3Item', {
    read: function () {
        this
            .Int16BE('apiKey')
            .Int16BE('minVersion')
            .Int16BE('maxVersion')
            .TaggedFields();
    }
});

// v4 (KAFKA-17011) — wire-identical to v3
Protocol.define('ApiVersionsRequestV4', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ApiVersionsRequest,
                apiVersion: 4,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.clientSoftwareName || '')
            .compactString(data.clientSoftwareVersion || '')
            .TaggedFields();
    }
});

// ApiVersions always uses response header v0 (just correlationId, no TaggedFields)
// regardless of version. This is a special case in the Kafka protocol — see KIP-511.
Protocol.define('ApiVersionsResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .compactArray('apiVersions', this.ApiVersionsResponseV3Item)
            .Int32BE('throttleTime')
            .TaggedFields();
    }
});
