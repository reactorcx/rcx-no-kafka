'use strict';

/* global describe, it, before, sinon */

var Kafka        = require('../lib/index');
var Protocol     = require('../lib/protocol');
var errors       = require('../lib/errors');
var utils        = require('../lib/utils');
var promiseUtils = require('../lib/promise-utils');
var Logger       = require('../lib/logger');

describe('Minor fixes (unit)', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    describe('utils.groupBy', function () {
        it('should handle a key named __proto__', function () {
            var key = '__proto' + '__';
            var result = utils.groupBy([{ topic: key, n: 1 }], function (x) { return x.topic; });
            result[key].should.be.an('array').with.length(1);
            result[key][0].n.should.equal(1);
        });
    });

    describe('promise-utils.mapConcurrent', function () {
        it('should default the concurrency when not provided', function () {
            return promiseUtils.mapConcurrent([1, 2, 3], function (x) { return x * 2; })
            .then(function (results) {
                results.should.eql([2, 4, 6]);
            });
        });

        it('should reject (not throw) when fn throws synchronously', function () {
            var p;
            (function () {
                p = promiseUtils.mapConcurrent([1], function () { throw new Error('sync boom'); }, 1);
            }).should.not.throw();
            return p.should.be.rejectedWith('sync boom');
        });
    });

    describe('error stack traces', function () {
        it('KafkaError should carry a stack trace', function () {
            var err = errors.byName('UnknownTopicOrPartition');
            err.should.have.property('stack').that.is.a('string');
            err.stack.should.contain('KafkaError');
        });

        it('NoKafkaConnectionError should carry a stack trace', function () {
            var err = new errors.NoKafkaConnectionError('b:1', 'down');
            err.should.have.property('stack').that.is.a('string');
        });
    });

    describe('logger default level', function () {
        it('should default to 3 (info), not trace', function () {
            new Logger({}).logLevel.should.equal(3);
        });
    });

    describe('parseHostString', function () {
        it('should default the port to 9092 when not specified', function () {
            var c = new Kafka.Producer({ clientId: 'unit' }).client;
            c.parseHostString('somehost').should.eql({ host: 'somehost', port: 9092 });
        });
    });

    describe('ConsumerGroup.commitOffset epoch guard', function () {
        it('should reject with RebalanceInProgress when not joined', function () {
            var cg = new Kafka.ConsumerGroup({ clientId: 'unit', groupId: 'cg-minor' });
            sinon.stub(cg.client, 'offsetCommitRequestV2').returns(Promise.resolve([]));
            return cg.commitOffset({ topic: 't', partition: 0, offset: 1 }).then(function () {
                throw new Error('expected rejection');
            }, function (err) {
                err.code.should.equal('RebalanceInProgress');
            });
        });
    });

    describe('BaseConsumer', function () {
        it('unsubscribe should not throw synchronously for an unknown topic', function () {
            var c = new Kafka.SimpleConsumer({ clientId: 'unit' });
            var p;
            (function () {
                p = c.unsubscribe('ghost-topic');
            }).should.not.throw();
            return p;
        });

        it('init called twice should start only one fetch loop', function () {
            var c = new Kafka.SimpleConsumer({ clientId: 'unit' });
            sinon.stub(c.client, 'init').returns(Promise.resolve());
            sinon.stub(c, '_fetch').returns(Promise.resolve());
            return c.init().then(function () {
                return c.init();
            }).then(function () {
                c._fetch.callCount.should.equal(1);
            });
        });
    });

    describe('Producer batch hash', function () {
        it('should not merge sends with different compression levels', function () {
            var p = new Kafka.Producer({ clientId: 'unit' });
            var levels = [];
            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'produceRequest').callsFake(function (reqs, codec, level) {
                levels.push(level);
                return Promise.resolve([{ topic: 't', partition: 0, offset: 0 }]);
            });
            return Promise.all([
                p.send({ topic: 't', partition: 0, message: { value: 'a' } }, { codec: 0, compressionLevel: 1 }),
                p.send({ topic: 't', partition: 0, message: { value: 'b' } }, { codec: 0, compressionLevel: 9 })
            ]).then(function () {
                levels.sort().should.eql([1, 9]);
            });
        });
    });

    describe('SyncGroup v4 assignment encoding', function () {
        it('should encode a missing assignment as empty bytes, not null', function () {
            var buf = protocol.write().SyncConsumerGroupRequestV4_GroupAssignment({ memberId: 'm' }).result;
            // compactString('m') = [0x02, 'm'], then compactBytes: empty = 0x01 (null would be 0x00)
            buf[2].should.equal(1);
        });
    });

    describe('OffsetCommitRequestV2 retentionTime default', function () {
        it('should not throw when retentionTime is omitted', function () {
            (function () {
                protocol.write().OffsetCommitRequestV2({
                    correlationId: 1,
                    clientId: 'c',
                    groupId: 'g',
                    generationId: 1,
                    memberId: 'm',
                    topics: []
                });
            }).should.not.throw();
        });
    });

    describe('KIP-848/932 heartbeat version checks', function () {
        it('consumerGroupHeartbeatRequest should reject clearly when the broker lacks the API', function () {
            var c = new Kafka.Producer({ clientId: 'unit' }).client;
            sinon.stub(c, '_findGroupCoordinator').returns(Promise.resolve({
                apiVersions: {}, // broker advertises versions, but not key 68
                server: function () { return 'old:9092'; }
            }));
            return c.consumerGroupHeartbeatRequest('g', 'm', 0, ['t'], 'uniform', [], 45000)
            .then(function () {
                throw new Error('expected rejection');
            }, function (err) {
                String(err.message).should.match(/does not support/i);
            });
        });

        it('shareGroupHeartbeatRequest should reject clearly when the broker lacks the API', function () {
            var c = new Kafka.Producer({ clientId: 'unit' }).client;
            sinon.stub(c, '_findGroupCoordinator').returns(Promise.resolve({
                apiVersions: {},
                server: function () { return 'old:9092'; }
            }));
            return c.shareGroupHeartbeatRequest('g', 'm', 0, null, ['t'])
            .then(function () {
                throw new Error('expected rejection');
            }, function (err) {
                String(err.message).should.match(/does not support/i);
            });
        });
    });

    describe('GroupAdmin.fetchConsumerLag', function () {
        it('should compute lag when the committed offset is 0', function () {
            var admin = new Kafka.GroupAdmin({ clientId: 'unit' });
            sinon.stub(admin.client, 'offsetFetchRequestV1').returns(Promise.resolve([
                { topic: 't', partition: 0, offset: 0 }
            ]));
            sinon.stub(admin, '_fetchHighWaterMark').returns(Promise.resolve([
                { topic: 't', partition: 0, highWaterMark: 10 }
            ]));
            return admin.fetchConsumerLag('g', [{ topicName: 't', partitions: [0] }]).then(function (r) {
                r[0].consumerLag.should.equal(10);
            });
        });
    });

    describe('assignment strategies with odd member metadata', function () {
        it('WeightedRoundRobin should assign partitions when Buffer metadata lacks weight', function () {
            var s = new Kafka.WeightedRoundRobinAssignmentStrategy();
            var result = s.assignment([{
                topic: 't',
                members: [{ id: 'm1', metadata: Buffer.from(JSON.stringify({})) }],
                partitions: [0, 1]
            }]);
            result.should.have.length(2);
            result.forEach(function (a) { a.memberId.should.equal('m1'); });
        });

        it('WeightedRoundRobin should not throw on malformed Buffer metadata', function () {
            var s = new Kafka.WeightedRoundRobinAssignmentStrategy();
            (function () {
                s.assignment([{
                    topic: 't',
                    members: [{ id: 'm1', metadata: Buffer.from('not json at all') }],
                    partitions: [0]
                }]);
            }).should.not.throw();
        });

        it('Consistent strategy should not throw on malformed Buffer metadata', function () {
            var s = new Kafka.ConsistentAssignmentStrategy();
            (function () {
                s.assignment([{
                    topic: 't',
                    members: [{ id: 'm1', metadata: Buffer.from('not json at all') }],
                    partitions: [0]
                }]);
            }).should.not.throw();
        });
    });
});
