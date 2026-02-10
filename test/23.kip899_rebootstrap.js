'use strict';

/* global describe, it, after */

var Client = require('../lib/client');

describe('KIP-899: Client Re-bootstrap', function () {
    describe('_rebootstrap()', function () {
        var client;

        after(function () {
            return client.end();
        });

        it('should close old connections and create fresh ones', function () {
            var oldBrokers;

            client = new Client({ connectionString: '127.0.0.1:9092' });

            return client.init().then(function () {
                oldBrokers = client.initialBrokers.slice();
                oldBrokers.length.should.be.greaterThan(0);

                return client._rebootstrap();
            }).then(function () {
                // same number of connections
                client.initialBrokers.length.should.equal(oldBrokers.length);
                // new connection objects (not same references)
                client.initialBrokers[0].should.not.equal(oldBrokers[0]);
                // old connections are closed
                oldBrokers[0].closed.should.equal(true);
                // new connections have API versions discovered
                (client.initialBrokers[0].apiVersions !== null).should.equal(true);
            });
        });
    });

    describe('updateMetadata() with rebootstrap enabled', function () {
        var client;

        after(function () {
            return client.end();
        });

        it('should recover when all initialBrokers are closed', function () {
            client = new Client({ connectionString: '127.0.0.1:9092', rebootstrap: true });

            return client.init().then(function () {
                // simulate total broker unavailability by closing all initial connections
                client.initialBrokers.forEach(function (c) { c.close(); });

                // updateMetadata should recover via rebootstrap
                return client.updateMetadata();
            }).then(function () {
                // verify metadata was refreshed
                Object.keys(client.brokerConnections).length.should.be.greaterThan(0);
            });
        });
    });

    describe('updateMetadata() with rebootstrap disabled', function () {
        var client;

        after(function () {
            return client.end();
        });

        it('should throw when all initialBrokers are closed', function () {
            client = new Client({ connectionString: '127.0.0.1:9092', rebootstrap: false });

            return client.init().then(function () {
                // close all initial connections — permanently dead (closed=true)
                client.initialBrokers.forEach(function (c) { c.close(); });

                return client.updateMetadata().then(function () {
                    throw new Error('should have thrown');
                }, function (err) {
                    // expected: all brokers unavailable, no rebootstrap recovery
                    (err !== null && err !== undefined).should.equal(true);
                });
            });
        });
    });
});
