//
// Created by Chaos Zhai on 12/7/23.
//
#include <mutex>
#include "eht.h"
#include "../lib/node/node.h"

template <typename K, typename V, typename KC>
class CoarseEHT : public ExtendibleHashTable<K, V, KC> {
public:
    explicit CoarseEHT(const std::string &name, const KC &cmp,
                       const HashFunction<K> &hash_fn,
                       uint32_t bucket_max_size = HTableBucketArraySize(sizeof(std::pair<K, V>)));

    auto Insert(const K &key, const V &value) -> bool;
    auto Remove(const K &key) -> bool;
    auto Get(const K &key) -> std::optional<V>;

private:
    std::mutex mutex_;
    Node *root_;
};
