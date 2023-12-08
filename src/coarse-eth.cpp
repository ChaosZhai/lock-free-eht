//
// Created by Chaos Zhai on 12/1/23.
//
#include "../include/coarse-eth.h"

template<typename K, typename V, typename KC>
CoarseEHT<K, V, KC>::CoarseEHT(const std::string &name, const KC &cmp,
                                     const HashFunction<K> &hash_fn, uint32_t bucket_max_size)
        : ExtendibleHashTable<K, V, KC>(name, cmp, hash_fn, bucket_max_size) {
            root_ = new Node();
        }


template<typename K, typename V, typename KC>
auto CoarseEHT<K, V, KC>::Insert(const K &key, const V &value) -> bool {
    std::scoped_lock<std::mutex> lock(mutex_);

    return ExtendibleHashTable<K, V, KC>::Insert(key, value);
}

template<typename K, typename V, typename KC>
auto CoarseEHT<K, V, KC>::Remove(const K &key) -> bool {
    std::scoped_lock<std::mutex> lock(mutex_);
    return ExtendibleHashTable<K, V, KC>::Remove(key);
}

template<typename K, typename V, typename KC>
auto CoarseEHT<K, V, KC>::Get(const K &key) -> std::optional<V> {
    std::scoped_lock<std::mutex> lock(mutex_);
    return ExtendibleHashTable<K, V, KC>::Get(key);
}