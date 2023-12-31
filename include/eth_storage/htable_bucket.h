//
// Created by Yicheng Zhang on 12/13/23.
//

#ifndef LOCK_FREE_EHT_HTABLE_BUCKET_H
#define LOCK_FREE_EHT_HTABLE_BUCKET_H

/**
 * Bucket node format:
 *  ----------------------------------------------------------------------------
 * | METADATA | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)
 *  ----------------------------------------------------------------------------
 *
 * Metadata format (size in byte, 8 bytes in total):
 *  --------------------------------
 * | CurrentSize (4) | MaxSize (4)
 *  --------------------------------
 */
#pragma once

#include <iostream>
#include <optional>
#include <utility>
#include <vector>

#include "../common.h"
#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>
#include "../../lib/node/leaf-node.hpp"
namespace eht {

/**
 * Bucket node for extendible hash table.
 */
    template<typename KeyType, typename ValueType, typename KeyComparator>
    class ExtendibleHTableBucket  {
    public:
        // Delete all constructor / destructor to ensure memory safety
        explicit ExtendibleHTableBucket(uint32_t max_size = HTableBucketArraySize(sizeof(MappingType))) {
            size_ = 0;
            max_size_ = max_size;
        }

        /**
         * Lookup a key
         *
         * @param key key to lookup
         * @param[out] value value to set
         * @param cmp the comparator
         * @return true if the key and value are present, false if not found.
         */
        auto Lookup(const KeyType &key, ValueType &value,
                    const KeyComparator &cmp) const -> bool {
            for (uint32_t i = 0; i < size_; i++) {
                if (cmp(array_[i].first, key) == 0) {
                    value = array_[i].second;
                    return true;
                }
            }
            return false;
        }
        auto Init(uint32_t max_size) {
            size_ = 0;
            max_size_ = max_size;
        }
        auto GetValueIndex(const KeyType &key, const KeyComparator &cmp) const
        -> uint32_t {
            for (uint32_t i = 0; i < size_; i++) {
                if (cmp(array_[i].first, key) == 0) {
                    return i;
                }
            }
            return size_;
        }

        /**
         * Attempts to insert a key and value in the bucket.
         *
         * @param key key to insert
         * @param value value to insert
         * @param cmp the comparator to use
         * @return true if inserted, false if bucket is full or the same key is
         * already present
         */
        auto Insert(const KeyType &key, const ValueType &value,
                    const KeyComparator &cmp) -> bool {
            if (IsFull()) {
                return false;
            }
            for (uint32_t i = 0; i < size_; ++i) {
                if (cmp(array_[i].first, key) == 0) {
                    return false; // Duplicate key found
                }
            }
            array_[size_] = std::make_pair(key, value);
            size_++;
            return true;
        }

        void PushBack(const KeyType &key, const ValueType &value,
                      const KeyComparator &cmp) {
            if (IsFull()) {
                return;
            }
            array_[size_] = std::make_pair(key, value);
            size_++;
        }

        /**
         * Removes a key and value.
         *
         * @return true if removed, false if not found
         */
        auto Remove(const KeyType &key, const KeyComparator &cmp) -> bool {
            for (uint32_t i = 0; i < size_; ++i) {
                if (cmp(array_[i].first, key) == 0) {
                    RemoveAt(i);
                    return true;
                }
            }
            return false;
        }

        void RemoveAt(uint32_t bucket_idx) {
            if (bucket_idx == size_ - 1) {
                size_--;
            } else if (bucket_idx < size_) {
                array_[bucket_idx] = array_[size_ - 1];
                size_--;
            }
        }

        /**
         * @brief Gets the key at an index in the bucket.
         *
         * @param bucket_idx the index in the bucket to get the key at
         * @return key at index bucket_idx of the bucket
         */
        auto KeyAt(uint32_t bucket_idx) const -> KeyType {
            return array_[bucket_idx].first;
        }

        /**
         * Gets the value at an index in the bucket.
         *
         * @param bucket_idx the index in the bucket to get the value at
         * @return value at index bucket_idx of the bucket
         */
        auto ValueAt(uint32_t bucket_idx) const -> ValueType {
            return array_[bucket_idx].second;
        }

        /**
         * Gets the entry at an index in the bucket.
         *
         * @param bucket_idx the index in the bucket to get the entry at
         * @return entry at index bucket_idx of the bucket
         */
        auto EntryAt(uint32_t bucket_idx) const
        -> const std::pair<KeyType, ValueType> & {
            return array_[bucket_idx];
        }

        auto Merge(ExtendibleHTableBucket<KeyType, ValueType, KeyComparator> *other)
        -> bool {
            if (size_ + other->size_ > max_size_) {
                return false;
            }
            for (uint32_t i = 0; i < other->size_; ++i) {
                array_[size_ + i] = other->array_[i];
            }
            size_ = size_ + other->size_;
            return true;
        }

        /**
         * @return number of entries in the bucket
         */
        [[nodiscard]] auto Size() const -> uint32_t { return size_; }

        /**
         * @return whether the bucket is full
         */
        [[nodiscard]] auto IsFull() const -> bool { return size_ >= max_size_; }

        /**
         * @return whether the bucket is almost full
         */
        [[nodiscard]] auto AlmostFull() const -> bool { return size_ >= max_size_ * 0.8; }

        /**
         * @return whether the bucket is empty
         */
        [[nodiscard]] auto IsEmpty() const -> bool { return size_ == 0; }

        /**
         * Prints the bucket's occupancy information
         */
        [[maybe_unused]] void PrintBucket() const {
            std::cout << "======== BUCKET (size_: " << size_
                      << " | max_size_: " << max_size_ << ") ========\n";
            std::cout << ("| i | k | v |\n");
            for (uint32_t idx = 0; idx < size_; idx++) {
                std::cout << "| " << idx << " | " << KeyAt(idx) << " | " << ValueAt(idx)
                          << " |\n";
            }
            std::cout << "================ END BUCKET ================\n";
            std::cout << "\n";
        }

    private:
        uint32_t size_;
        uint32_t max_size_;
        MappingType array_[HTableBucketArraySize(sizeof(MappingType))];
    };

#endif // LOCK_FREE_EHT_HTABLE_BUCKET_H
} // namespace eht
