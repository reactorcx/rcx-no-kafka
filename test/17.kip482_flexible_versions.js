'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');
var globals  = require('../lib/protocol/globals');

describe('KIP-482 Flexible Versions Infrastructure', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    /////////////////////
    // Compact String  //
    /////////////////////

    describe('compactString', function () {
        it('should round-trip a non-null string', function () {
            var encoded = protocol.write().compactString('hello').result;
            var decoded = protocol.read(encoded).compactString('value').result;
            decoded.value.should.equal('hello');
            // UVarint(6) = 0x06, then "hello"
            encoded[0].should.equal(0x06);
            encoded.length.should.equal(6);
        });

        it('should round-trip null', function () {
            var encoded = protocol.write().compactString(null).result;
            var decoded = protocol.read(encoded).compactString('value').result;
            should.equal(decoded.value, null);
            encoded.length.should.equal(1);
            encoded[0].should.equal(0x00);
        });

        it('should round-trip empty string', function () {
            var encoded = protocol.write().compactString('').result;
            var decoded = protocol.read(encoded).compactString('value').result;
            decoded.value.should.equal('');
            encoded.length.should.equal(1);
            encoded[0].should.equal(0x01);
        });

        it('should round-trip multi-byte UTF-8', function () {
            var encoded = protocol.write().compactString('\u00e9').result; // é = 2 bytes in UTF-8
            var decoded = protocol.read(encoded).compactString('value').result;
            decoded.value.should.equal('\u00e9');
            encoded[0].should.equal(0x03); // UVarint(3) = length 2 + 1
        });
    });

    //////////////////////////////
    // Compact Nullable String  //
    //////////////////////////////

    describe('compactNullableString', function () {
        it('should round-trip a non-null string', function () {
            var encoded = protocol.write().compactNullableString('test').result;
            var decoded = protocol.read(encoded).compactNullableString('value').result;
            decoded.value.should.equal('test');
        });

        it('should round-trip null', function () {
            var encoded = protocol.write().compactNullableString(null).result;
            var decoded = protocol.read(encoded).compactNullableString('value').result;
            should.equal(decoded.value, null);
        });

        it('should round-trip empty string', function () {
            var encoded = protocol.write().compactNullableString('').result;
            var decoded = protocol.read(encoded).compactNullableString('value').result;
            decoded.value.should.equal('');
        });
    });

    ////////////////////
    // Compact Bytes  //
    ////////////////////

    describe('compactBytes', function () {
        it('should round-trip a non-null buffer', function () {
            var buf = new Buffer([0x01, 0x02, 0x03]);
            var encoded = protocol.write().compactBytes(buf).result;
            var decoded = protocol.read(encoded).compactBytes('value').result;
            Buffer.isBuffer(decoded.value).should.equal(true);
            decoded.value.length.should.equal(3);
            decoded.value[0].should.equal(0x01);
            decoded.value[1].should.equal(0x02);
            decoded.value[2].should.equal(0x03);
        });

        it('should round-trip null', function () {
            var encoded = protocol.write().compactBytes(null).result;
            var decoded = protocol.read(encoded).compactBytes('value').result;
            should.equal(decoded.value, null);
            encoded[0].should.equal(0x00);
        });

        it('should round-trip empty buffer', function () {
            var encoded = protocol.write().compactBytes(new Buffer(0)).result;
            var decoded = protocol.read(encoded).compactBytes('value').result;
            Buffer.isBuffer(decoded.value).should.equal(true);
            decoded.value.length.should.equal(0);
            encoded[0].should.equal(0x01);
        });
    });

    /////////////////////////////
    // Compact Nullable Bytes  //
    /////////////////////////////

    describe('compactNullableBytes', function () {
        it('should round-trip a non-null buffer', function () {
            var buf = new Buffer([0xAA, 0xBB]);
            var encoded = protocol.write().compactNullableBytes(buf).result;
            var decoded = protocol.read(encoded).compactNullableBytes('value').result;
            Buffer.isBuffer(decoded.value).should.equal(true);
            decoded.value.length.should.equal(2);
        });

        it('should round-trip null', function () {
            var encoded = protocol.write().compactNullableBytes(null).result;
            var decoded = protocol.read(encoded).compactNullableBytes('value').result;
            should.equal(decoded.value, null);
        });

        it('should round-trip empty buffer', function () {
            var encoded = protocol.write().compactNullableBytes(new Buffer(0)).result;
            var decoded = protocol.read(encoded).compactNullableBytes('value').result;
            Buffer.isBuffer(decoded.value).should.equal(true);
            decoded.value.length.should.equal(0);
        });
    });

    ////////////////////
    // Compact Array  //
    ////////////////////

    describe('compactArray', function () {
        it('should round-trip a non-empty array', function () {
            var encoded = protocol.write().compactArray(['a', 'b'], function (item) {
                this.compactString(item);
            }).result;
            var decoded = protocol.read(encoded).compactArray('items', function () {
                this.compactString('value');
            }).result;
            decoded.items.length.should.equal(2);
            decoded.items[0].value.should.equal('a');
            decoded.items[1].value.should.equal('b');
        });

        it('should round-trip null', function () {
            var encoded = protocol.write().compactArray(null, function (item) {
                this.compactString(item);
            }).result;
            var decoded = protocol.read(encoded).compactArray('items', function () {
                this.compactString('value');
            }).result;
            should.equal(decoded.items, null);
            encoded[0].should.equal(0x00);
        });

        it('should round-trip empty array', function () {
            var encoded = protocol.write().compactArray([], function (item) {
                this.compactString(item);
            }).result;
            var decoded = protocol.read(encoded).compactArray('items', function () {
                this.compactString('value');
            }).result;
            decoded.items.length.should.equal(0);
            encoded[0].should.equal(0x01);
        });
    });

    /////////////////////////////
    // Compact Nullable Array  //
    /////////////////////////////

    describe('compactNullableArray', function () {
        it('should round-trip a non-empty array', function () {
            var encoded = protocol.write().compactNullableArray(['x'], function (item) {
                this.compactString(item);
            }).result;
            var decoded = protocol.read(encoded).compactNullableArray('items', function () {
                this.compactString('value');
            }).result;
            decoded.items.length.should.equal(1);
            decoded.items[0].value.should.equal('x');
        });

        it('should round-trip null', function () {
            var encoded = protocol.write().compactNullableArray(null, function (item) {
                this.compactString(item);
            }).result;
            var decoded = protocol.read(encoded).compactNullableArray('items', function () {
                this.compactString('value');
            }).result;
            should.equal(decoded.items, null);
        });

        it('should round-trip empty array', function () {
            var encoded = protocol.write().compactNullableArray([], function (item) {
                this.compactString(item);
            }).result;
            var decoded = protocol.read(encoded).compactNullableArray('items', function () {
                this.compactString('value');
            }).result;
            decoded.items.length.should.equal(0);
        });
    });

    ///////////////////
    // Tagged Fields //
    ///////////////////

    describe('TaggedFields', function () {
        it('should write a single 0x00 byte (empty tags)', function () {
            var encoded = protocol.write().TaggedFields().result;
            encoded.length.should.equal(1);
            encoded[0].should.equal(0x00);
        });

        it('should skip unknown tags on read', function () {
            // Craft a buffer with 2 tagged fields:
            // UVarint(2) = tag count
            // Tag 0: UVarint(0) tag, UVarint(3) size, 3 bytes data
            // Tag 1: UVarint(1) tag, UVarint(2) size, 2 bytes data
            // Then a trailing marker byte 0xFF to verify we land at the right offset
            var buf = new Buffer([
                0x02,                   // tagCount = 2
                0x00, 0x03, 0xAA, 0xBB, 0xCC,  // tag=0, size=3, data
                0x01, 0x02, 0xDD, 0xEE,         // tag=1, size=2, data
                0xFF                    // trailing marker
            ]);
            var result = protocol.read(buf).TaggedFields('_tags').Int8('marker').result;
            result.marker.should.equal(-1); // 0xFF as signed Int8
        });
    });

    /////////////////////////////
    // Flexible Request Header //
    /////////////////////////////

    describe('FlexibleRequestHeader', function () {
        it('should encode apiKey, apiVersion, correlationId, compactString clientId, and empty tags', function () {
            var encoded = protocol.write().FlexibleRequestHeader({
                apiKey: 3,
                apiVersion: 9,
                correlationId: 42,
                clientId: 'test'
            }).result;

            // Int16BE apiKey = 0x00, 0x03
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(0x03);
            // Int16BE apiVersion = 0x00, 0x09
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(0x09);
            // Int32BE correlationId = 0x00, 0x00, 0x00, 0x2A
            encoded[4].should.equal(0x00);
            encoded[5].should.equal(0x00);
            encoded[6].should.equal(0x00);
            encoded[7].should.equal(0x2A);
            // compactNullableString "test": UVarint(5) = 0x05, then "test"
            encoded[8].should.equal(0x05);
            encoded[9].should.equal(0x74); // 't'
            encoded[10].should.equal(0x65); // 'e'
            encoded[11].should.equal(0x73); // 's'
            encoded[12].should.equal(0x74); // 't'
            // TaggedFields: UVarint(0) = 0x00
            encoded[13].should.equal(0x00);
            encoded.length.should.equal(14);
        });

        it('should encode null clientId', function () {
            var encoded = protocol.write().FlexibleRequestHeader({
                apiKey: 18,
                apiVersion: 3,
                correlationId: 1,
                clientId: null
            }).result;

            // 2 + 2 + 4 + 1 (null compact string) + 1 (empty tags) = 10
            encoded.length.should.equal(10);
            encoded[8].should.equal(0x00); // null compactNullableString
            encoded[9].should.equal(0x00); // empty TaggedFields
        });
    });

    /////////////////////////////////////
    // FLEXIBLE_VERSION_THRESHOLDS map //
    /////////////////////////////////////

    describe('FLEXIBLE_VERSION_THRESHOLDS', function () {
        it('should have correct thresholds for key APIs', function () {
            var thresholds = globals.FLEXIBLE_VERSION_THRESHOLDS;
            should.equal(thresholds[0], null);  // Produce: not flexible
            should.equal(thresholds[1], null);  // Fetch: not flexible
            thresholds[3].should.equal(9);      // Metadata
            thresholds[10].should.equal(3);     // FindCoordinator
            thresholds[18].should.equal(3);     // ApiVersions
            thresholds[22].should.equal(2);     // InitProducerId
        });
    });
});
