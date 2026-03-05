'use strict';

var util = require('util');

// Log levels: 0=none, 1=error, 2=warn, 3=log(info), 4=debug, 5=trace
var LEVELS = { error: 1, warn: 2, log: 3, debug: 4, trace: 5 };

function Logger(options) {
    options = options || {};
    this.logLevel = options.logLevel !== undefined ? options.logLevel : 5;
    this.logFunction = options.logFunction || null;
}

Logger.prototype._log = function (level, args) {
    var i, msg;

    if (LEVELS[level] > this.logLevel) {
        return;
    }

    if (this.logFunction) {
        if (typeof this.logFunction === 'function') {
            this.logFunction.apply(null, args);
        } else if (typeof this.logFunction[level] === 'function') {
            this.logFunction[level].apply(this.logFunction, args);
        }
        return;
    }

    msg = '';
    for (i = 0; i < args.length; i++) {
        if (i > 0) { msg += ' '; }
        if (args[i] instanceof Error) {
            msg += args[i].stack || args[i].toString();
        } else if (typeof args[i] === 'object' && args[i] !== null) {
            msg += util.inspect(args[i]);
        } else {
            msg += String(args[i]);
        }
    }

    if (level === 'error') {
        process.stderr.write('[' + level + '] ' + msg + '\n');
    } else {
        process.stdout.write('[' + level + '] ' + msg + '\n');
    }
};

Logger.prototype.log = function () {
    this._log('log', arguments);
};

Logger.prototype.debug = function () {
    this._log('debug', arguments);
};

Logger.prototype.trace = function () {
    this._log('trace', arguments);
};

Logger.prototype.error = function () {
    this._log('error', arguments);
};

Logger.prototype.warn = function () {
    this._log('warn', arguments);
};

module.exports = Logger;
