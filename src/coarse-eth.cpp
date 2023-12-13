//
// Created by Chaos Zhai on 12/1/23.
//
#include "../include/coarse-eth.h"

template<typename K, typename V, typename KC>
CoarseEHT<K, V, KC>::CoarseEHT(const std::string &name, const KC &cmp,
                                     const HashFunction<K> &hash_fn, uint32_t bucket_max_size)
        : ExtendibleHashTable<K, V, KC>(name, cmp, hash_fn, bucket_max_size) {
            root_ = new InnerNode();
        }


template<typename K, typename V, typename KC>
auto CoarseEHT<K, V, KC>::Insert(const K &key, const V &value) -> bool {
    std::scoped_lock<std::mutex> lock(mutex_);
    uint32_t hash_val = Hash(key);
    uint32_t dir_idx = Node::GetDirIndex(hash_val, true);
    auto dir = root_->GetNode(dir_idx)->AsMut<InnerNode>();
    uint32_t bucket_idx = Node::GetDirIndex(hash_val, false);
    auto bucket = dir->GetNode(bucket_idx)->AsMut<LeafNode<K, V, KC>>();
    return bucket->Insert(key, value);
}

template<typename K, typename V, typename KC>
auto CoarseEHT<K, V, KC>::Remove(const K &key) -> bool {
    std::scoped_lock<std::mutex> lock(mutex_);
    return ExtendibleHashTable<K, V, KC>::Remove(key);
}

template<typename K, typename V, typename KC>
auto CoarseEHT<K, V, KC>::Get(const K &key) -> std::optional<V> {
    std::scoped_lock<std::mutex> lock(mutex_);
    uint32_t hash_val = Hash(key);
    uint32_t dir_idx = Node::GetDirIndex(hash_val, true);
    auto dir = root_->GetNode(dir_idx)->As<InnerNode>();
    uint32_t bucket_idx = Node::GetDirIndex(hash_val, false);
    auto bucket = dir->GetNode(bucket_idx)->As<LeafNode<K, V, KC>>();
    return bucket->Get(key);
}