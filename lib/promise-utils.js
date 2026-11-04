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
 * Reject with `message` if `promise` hasn't settled within `ms`.
 * The source promise is left to settle on its own; this only races it against a timer.
 */
function timeout(promise, ms, message) {
    return new Promise(function (resolve, reject) {
        var timer = setTimeout(function () {
            reject(new Error(message || ('Operation timed out after ' + ms + 'ms')));
        }, ms);
        // Do not hold the event loop open on this alone: after end() a pending drain timer can be
        // the last live handle, and the process would sit out the full timeout before exiting.
        // During a real rebalance the broker sockets keep the loop alive and this still fires.
        if (typeof timer.unref === 'function') { timer.unref(); }
        Promise.resolve(promise).then(function (value) {
            clearTimeout(timer);
            resolve(value);
        }, function (err) {
            clearTimeout(timer);
            reject(err);
        });
    });
}

/**
 * Run fn over arr with limited concurrency.
 * Replaces Promise.map(arr, fn, {concurrency: N}).
 */
function mapConcurrent(arr, fn, concurrency) {
    var results = new Array(arr.length);
    var index = 0;
    var failed = false;
    var workers = [];
    var i;

    function next() {
        var j;
        if (failed) { return Promise.resolve(); }
        j = index++;
        if (j >= arr.length) {
            return Promise.resolve();
        }
        return Promise.resolve(fn(arr[j], j)).then(function (val) {
            results[j] = val;
            return next();
        }, function (err) {
            failed = true;
            throw err;
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
    timeout: timeout,
    mapConcurrent: mapConcurrent
};
