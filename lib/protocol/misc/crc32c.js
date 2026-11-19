'use strict';

// CRC-32C (Castagnoli) lookup tables for slicing-by-8, polynomial 0x82F63B78 (reflected):
// table k (at k * 256) gives a byte's CRC after k more zero bytes
var T = new Int32Array(2048);

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
        T[i] = crc;
    }
    for (i = 0; i < 256; i++) {
        for (j = 256; j < 2048; j += 256) {
            T[j + i] = (T[j - 256 + i] >>> 8) ^ T[T[j - 256 + i] & 0xFF];
        }
    }
}());

/**
 * Calculates the CRC-32C (Castagnoli) checksum of a Buffer, 8 bytes per step
 * (about 5x a byte at a time; Node has no CRC-32C built in).
 *
 * @param {Buffer} buf
 * @return {Number} unsigned 32-bit CRC-32C value
 */
module.exports = function (buf) {
    var crc = -1, i = 0, l = buf.length, end = l - (l & 7);
    for (; i < end; i += 8) {
        crc ^= buf[i] | (buf[i + 1] << 8) | (buf[i + 2] << 16) | (buf[i + 3] << 24);
        crc = T[1792 + (crc & 0xFF)] ^ T[1536 + ((crc >>> 8) & 0xFF)] ^ T[1280 + ((crc >>> 16) & 0xFF)] ^ T[1024 + (crc >>> 24)] ^
            T[768 + buf[i + 4]] ^ T[512 + buf[i + 5]] ^ T[256 + buf[i + 6]] ^ T[buf[i + 7]];
    }
    for (; i < l; i++) {
        crc = (crc >>> 8) ^ T[(crc ^ buf[i]) & 0xFF];
    }
    return (crc ^ -1) >>> 0;
};
