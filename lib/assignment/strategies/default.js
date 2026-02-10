'use strict';


function DefaultAssignmentStrategy() {
}

// simple round robin assignment strategy
DefaultAssignmentStrategy.prototype.assignment = function (subscriptions) { // [{topic:String, members:[], partitions:[]}]
    var result = [];

    subscriptions.forEach(function (sub) {
        sub.partitions.forEach(function (p) {
            result.push({
                topic: sub.topic,
                partition: p,
                memberId: sub.members[p % sub.members.length].id
            });
        });
    });

    return result;
};

module.exports = DefaultAssignmentStrategy;
