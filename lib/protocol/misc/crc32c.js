'use strict';

// CRC-32C (Castagnoli) lookup table using polynomial 0x82F63B78 (reflected)
var crc32ctab = new Array(256);

(function () {
    var i, j, crc;
    for (i = 0; i < 256; i++) {
        crc = i;
        for (j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >>> 1) ^ 0x82F63B78;
            } else {
                crc = crc >>> 1;
            }
        }
        crc32ctab[i] = crc;
    }
}());

/**
 * Calculates the CRC-32C (Castagnoli) checksum of a Buffer.
 *
 * @param {Buffer} buf
 * @return {Number} unsigned 32-bit CRC-32C value
 */
module.exports = function (buf) {
    var crc = 0xFFFFFFFF, i, l;
    for (i = 0, l = buf.length; i < l; i++) {
        crc = (crc >>> 8) ^ crc32ctab[(crc ^ buf[i]) & 0xFF];
    }
    return (crc ^ 0xFFFFFFFF) >>> 0;
};
