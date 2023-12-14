//
// Created by Chaos Zhai on 12/13/23.
//
#pragma once
#include <atomic>
#include "node.h"

namespace eht {

    const int kMaxLevel = 4;
    const int kSegmentSize = 64;

    using SegmentIndex = size_t;
    typedef std::atomic<DummyNode *> Bucket;

    class Segment {
    public:
        Segment() : level(1), data(nullptr) {}

        explicit Segment(int level_) : level(level_), data(nullptr) {}

        Bucket *get_sub_buckets() const {
            return static_cast<Bucket *>(data.load(std::memory_order_consume));
        }

        Segment *get_sub_segments() const {
            return static_cast<Segment *>(data.load(std::memory_order_consume));
        }

        ~Segment() {
            void *ptr = data.load(std::memory_order_consume);
            if (nullptr == ptr) return;

            if (level == kMaxLevel - 1) {
                auto *buckets = static_cast<Bucket *>(ptr);
                delete[] buckets;
            } else {
                auto *sub_segments = static_cast<Segment *>(ptr);
                delete[] sub_segments;
            }
        }

        int level;                // Level of segment.
        std::atomic<void *> data;  // If level == kMaxLevel then data point to
        // buckets else data point to segments.
    };

    /**
     * Create a new segment array.
     * @param level
     * @return
     */
    Segment *NewSegments(int level) {
        auto *segments = new Segment[kSegmentSize];
        for (int i = 0; i < kSegmentSize; ++i) {
            segments[i].level = level;
            segments[i].data.store(nullptr, std::memory_order_release);
        }
        return segments;
    }

    /**
     * Create a new bucket array.
     * @return
     */
    Bucket *NewBuckets() {
        auto *buckets = new Bucket[kSegmentSize];
        for (int i = 0; i < kSegmentSize; ++i) {
            buckets[i].store(nullptr, std::memory_order_release);
        }
        return buckets;
    }

    // When the table size is 2^i , a logical table bucket b contains items whose
    // keys k maintain k mod 2^i = b. When the size becomes 2^i+1, the items of
    // this bucket are split into two buckets: some remain in the bucket b, and
    // others, for which k mod 2^(i+1) == b + 2^i.
    BucketIndex GetBucketParent(BucketIndex bucket_index) {
        //__builtin_clzl: Get number of leading zero bits.
        // Unset the MSB(most significant bit) of bucket_index;
        return (~(mask >> (__builtin_clzl(bucket_index))) &
                bucket_index);
    };

}  // namespace eht