//
// Created by Chaos Zhai on 12/14/23.
//
#pragma once
#include <cmath>


namespace eht {
    // Fast reverse bits using Lookup Table.
#define R2(n) n, n + 2 * 64, n + 1 * 64, n + 3 * 64
#define R4(n) R2(n), R2(n + 2 * 16), R2(n + 1 * 16), R2(n + 3 * 16)
#define R6(n) R4(n), R4(n + 2 * 4), R4(n + 1 * 4), R4(n + 3 * 4)
// Lookup Table that store the reverse of each 8bit number.
    size_t reverse8bits_[256] = {R6(0), R6(2), R6(1), R6(3)};

    static size_t Reverse(size_t hash) {
        return reverse8bits_[hash & 0xff] << 56 |
               reverse8bits_[(hash >> 8) & 0xff] << 48 |
               reverse8bits_[(hash >> 16) & 0xff] << 40 |
               reverse8bits_[(hash >> 24) & 0xff] << 32 |
               reverse8bits_[(hash >> 32) & 0xff] << 24 |
               reverse8bits_[(hash >> 40) & 0xff] << 16 |
               reverse8bits_[(hash >> 48) & 0xff] << 8 |
               reverse8bits_[(hash >> 56) & 0xff];
    }

} // namespace eht