'use strict';

var crc32    = require('../../protocol/misc/crc32');
var util     = require('util');
var DefaultPartitioner = require('./default');

function HashCRC32Partitioner() {
    DefaultPartitioner.apply(this, arguments);
}

util.inherits(HashCRC32Partitioner, DefaultPartitioner);

HashCRC32Partitioner.prototype.partition = function (_topicName, partitions, message) {
    var crc;

    if (message.key === undefined || message.key === null) {
        return 0;
    }

    // crc32() reads the low byte of each char code: a Buffer as latin1 gives its bytes; a string
    // key keeps its old hashing (its bytes only if ASCII); anything else is sent as String(key)
    crc = crc32(Buffer.isBuffer(message.key) ? message.key.toString('latin1') : String(message.key));
    return crc % partitions.length;
};

module.exports = HashCRC32Partitioner;
