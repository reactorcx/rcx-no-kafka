'use strict';

/**
 * Recursively merge defaults into target. Only fills in missing keys.
 * Replaces lodash.defaultsDeep.
 */
function defaultsDeep(target, defaults) {
    var keys, i, key;
    if (target === undefined || target === null) {
        target = {};
    }
    keys = Object.keys(defaults);
    for (i = 0; i < keys.length; i++) {
        key = keys[i];
        if (target[key] === undefined) {
            target[key] = defaults[key];
        } else if (typeof defaults[key] === 'object' && defaults[key] !== null && !Array.isArray(defaults[key])
                   && typeof target[key] === 'object' && target[key] !== null && !Array.isArray(target[key])) {
            defaultsDeep(target[key], defaults[key]);
        }
    }
    return target;
}

/**
 * Remove trailing elements that match predicate from an array.
 * If predicate is an object, matches properties.
 * Replaces lodash.dropRightWhile.
 */
function dropRightWhile(arr, predicate) {
    var i, fn, keys, j;
    if (typeof predicate === 'function') {
        fn = predicate;
    } else {
        // object predicate: match all own properties
        keys = Object.keys(predicate);
        fn = function (item) {
            for (j = 0; j < keys.length; j++) {
                if (item[keys[j]] !== predicate[keys[j]]) {
                    return false;
                }
            }
            return true;
        };
    }
    i = arr.length;
    while (i > 0 && fn(arr[i - 1])) {
        i--;
    }
    return arr.slice(0, i);
}

exports.defaultsDeep = defaultsDeep;
exports.dropRightWhile = dropRightWhile;
