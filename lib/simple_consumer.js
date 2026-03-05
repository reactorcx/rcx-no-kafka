'use strict';

var utils        = require('./utils');
var BaseConsumer = require('./base_consumer');
var util         = require('util');

function SimpleConsumer(options) {
    this.options = utils.defaultsDeep(options || {}, {
        groupId: 'no-kafka-group-v0'
    });

    BaseConsumer.call(this, this.options);
}

module.exports = SimpleConsumer;

util.inherits(SimpleConsumer, BaseConsumer);

/**
 * Initialize SimpleConsumer
 *
 * @return {Prommise}
 */
SimpleConsumer.prototype.init = function () {
    return BaseConsumer.prototype.init.call(this);
};

SimpleConsumer.prototype._prepareOffsetRequest = function (type, commits) {
    var self = this;

    if (!Array.isArray(commits)) {
        commits = [commits];
    }

    return Promise.all(commits.map(function (commit) {
        if (self.subscriptions[commit.topic + ':' + commit.partition]) {
            commit.leader = self.subscriptions[commit.topic + ':' + commit.partition].leader;
            return null;
        }
        return self.client.findLeader(commit.topic, commit.partition).then(function (leader) {
            commit.leader = leader;
        });
    }))
    .then(function () {
        var grouped = utils.groupBy(commits, function (c) { return c.leader; });
        var result = {}, leaders = Object.keys(grouped), i, topics, topicKeys, j;
        for (i = 0; i < leaders.length; i++) {
            topics = utils.groupBy(grouped[leaders[i]], function (c) { return c.topic; });
            topicKeys = Object.keys(topics);
            result[leaders[i]] = [];
            for (j = 0; j < topicKeys.length; j++) {
                result[leaders[i]].push({
                    topicName: topicKeys[j],
                    partitions: type === 'fetch' ? topics[topicKeys[j]].map(function (p) { return p.partition; }) : topics[topicKeys[j]]
                });
            }
        }
        return result;
    });
};

/**
 * Commit (save) processed offsets to Kafka (Zookeeper)
 *
 * @param  {Object|Array} commits [{topic, partition, offset, metadata}]
 * @return {Promise}
 */
SimpleConsumer.prototype.commitOffset = function (commits) {
    var self = this;

    return self._prepareOffsetRequest('commit', commits).then(function (data) {
        return self.client.offsetCommitRequestV0(self.options.groupId, data);
    });
};

/**
 * Fetch commited (saved) offsets [{topic, partition}]
 *
 * @param  {Object|Array} commits
 * @return {Promise}
 */
SimpleConsumer.prototype.fetchOffset = function (commits) {
    var self = this;

    return self._prepareOffsetRequest('fetch', commits).then(function (data) {
        return self.client.offsetFetchRequestV0(self.options.groupId, data);
    });
};
