'use strict';

/* global describe, it */

var errors = require('../lib/errors');

// Mappings verified against apache/kafka 4.1 Errors.java
describe('Errors (unit)', function () {
    describe('corrected error code mappings', function () {
        var expected = [
            [52, 'TransactionCoordinatorFenced'],
            [67, 'InvalidPrincipalType'],
            [76, 'UnsupportedCompressionType'],
            [90, 'ProducerFenced'],
            [120, 'TransactionAbortable'],
            [121, 'InvalidRecordState'],
            [122, 'ShareSessionNotFound'],
            [123, 'InvalidShareSessionEpoch']
        ];

        expected.forEach(function (pair) {
            it('should map code ' + pair[0] + ' to ' + pair[1], function () {
                var err = errors.byCode(pair[0]);
                err.should.be.an.instanceOf(errors.KafkaError);
                err.code.should.equal(pair[1]);
            });
        });
    });

    describe('full code coverage 1-133', function () {
        it('should map every broker error code without falling through to a plain Error', function () {
            var code, err;
            for (code = 1; code <= 133; code++) {
                err = errors.byCode(code);
                err.should.be.an.instanceOf(errors.KafkaError, 'code ' + code + ' is unmapped');
            }
        });
    });
});
