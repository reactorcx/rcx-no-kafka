'use strict';


var WRRPool = require('wrr-pool');
var util    = require('util');
var DefaultAssignmentStrategy = require('./default');

function WeightedRoundRobinAssignmentStrategy() {
}

util.inherits(WeightedRoundRobinAssignmentStrategy, DefaultAssignmentStrategy);

WeightedRoundRobinAssignmentStrategy.prototype.assignment = function (subscriptions) { // [{topic:String, members:[], partitions:[]}]
    var result = [];

    subscriptions.forEach(function (sub) {
        var members = new WRRPool();
        sub.members.forEach(function (member) {
            var weight = 10, m;
            if (Buffer.isBuffer(member.metadata)) {
                m = JSON.parse(member.metadata);
                weight = m.weight;
            }
            members.add(member.id, weight);
        });

        sub.partitions.forEach(function (p) {
            result.push({
                topic: sub.topic,
                partition: p,
                memberId: members.next()
            });
        });
    });

    return result;
};

module.exports = WeightedRoundRobinAssignmentStrategy;
