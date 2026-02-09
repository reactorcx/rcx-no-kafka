'use strict';

/**
 * Delay for a given number of milliseconds.
 * Replaces Promise.delay(ms).
 */
function delay(ms) {
    return new Promise(function (resolve) {
        setTimeout(resolve, ms);
    });
}

/**
 * Returns a function suitable for .then(delayChain(ms))
 * that waits ms milliseconds then passes through the value.
 * Replaces .delay(ms) chained on a Bluebird promise.
 */
function delayChain(ms) {
    return function (value) {
        return new Promise(function (resolve) {
            setTimeout(function () { resolve(value); }, ms);
        });
    };
}

/**
 * Run fn over arr with limited concurrency.
 * Replaces Promise.map(arr, fn, {concurrency: N}).
 */
function mapConcurrent(arr, fn, concurrency) {
    var results = new Array(arr.length);
    var index = 0;
    var workers = [];
    var i;

    function next() {
        var j = index++;
        if (j >= arr.length) {
            return Promise.resolve();
        }
        return Promise.resolve(fn(arr[j], j)).then(function (val) {
            results[j] = val;
            return next();
        });
    }

    for (i = 0; i < Math.min(concurrency, arr.length); i++) {
        workers.push(next());
    }

    return Promise.all(workers).then(function () {
        return results;
    });
}

module.exports = {
    delay: delay,
    delayChain: delayChain,
    mapConcurrent: mapConcurrent
};
