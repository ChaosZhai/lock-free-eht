//
// Created by Chaos Zhai on 12/1/23.
//
#pragma once

#include <string>
#include <utility>
#include "hash_function.h"

#ifndef LOCK_FREE_EHT_EHT_H
#define LOCK_FREE_EHT_EHT_H

#endif //LOCK_FREE_EHT_EHT_H

#define HTABLE_HEADER_MAX_DEPTH 9
#define HTABLE_DIRECTORY_MAX_DEPTH 9
#define HTableBucketArraySize(x) (1000 / (x))
namespace eht {

    template<typename K, typename V, typename KC>
    class ExtendibleHashTable {
    public:
        ExtendibleHashTable() = default;
        /**
         * @brief Creates a new ExtendibleHashTable.
         *
         * @param name
         * @param cmp comparator for keys
         * @param hash_fn the hash function
         * @param bucket_max_size the max size allowed for the bucket array
         */
        explicit ExtendibleHashTable(std::string name, const KC &cmp,
                                     const HashFunction<K> &hash_fn,
                                     uint32_t bucket_max_size = HTableBucketArraySize(sizeof(std::pair<K, V>)))
                : name_(std::move(name)), cmp_(cmp), hash_fn_(hash_fn) {}

        virtual auto Insert(const K &key, const V &value) -> bool = 0;

        virtual auto Remove(const K &key) -> bool = 0;

        virtual auto Get(const K &key) -> std::optional<V> = 0;

    protected:
        /**
         * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
         * for extendible hashing.
         *
         * @param key the key to hash
         * @return the down-casted 32-bit hash
         */
        auto Hash(K key) const -> uint32_t {
            return static_cast<uint32_t>((key));
        }

        std::string name_;
        KC cmp_;
        HashFunction<K> hash_fn_;

    private:

    };
}  // namespace eht
