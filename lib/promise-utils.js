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
    var failed = false;
    var workers = [];
    var i;

    if (!(concurrency >= 1)) {
        concurrency = 10; // an invalid/missing limit must not silently skip the work
    }

    function next() {
        var j;
        if (failed) { return Promise.resolve(); }
        j = index++;
        if (j >= arr.length) {
            return Promise.resolve();
        }
        return new Promise(function (resolve) {
            // fn runs inside the executor so synchronous throws become rejections
            resolve(fn(arr[j], j));
        }).then(function (val) {
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

/**
 * Resolves when p settles (either way) or after ms, whichever is first.
 */
function within(p, ms) {
    return new Promise(function (resolve) {
        var timer = setTimeout(resolve, ms);
        Promise.resolve(p).catch(function () { return null; }).then(function () {
            clearTimeout(timer);
            resolve();
        });
    });
}

module.exports = {
    delay: delay,
    delayChain: delayChain,
    mapConcurrent: mapConcurrent,
    within: within
};
