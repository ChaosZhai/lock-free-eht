//
// Created by Chaos Zhai on 12/7/23.
//
#pragma once
#include <mutex>
#include <optional>
#include <utility>

#include "eht.h"
#include "../lib/node/inner-node.hpp"
#include "../lib/node/leaf-node.hpp"
#include "eth_storage/htable_bucket.h"
#include "eth_storage/htable_directory.h"
#include "eth_storage/htable_header.h"


namespace eht {
template <typename K, typename V, typename KC>
class CoarseEHT : public ExtendibleHashTable<K, V, KC> {
public:
    explicit CoarseEHT(std::string name, const KC &cmp,
                       const HashFunction<K> &hash_fn,
                       uint32_t bucket_max_size = HTableBucketArraySize(sizeof(std::pair<K, V>)))
                       : cmp_(cmp), hash_fn_(hash_fn), bucket_max_size_(bucket_max_size),name_(std::move(name)) {
        root_ = std::make_shared<ExtendibleHTableHeaderNode>().get();
    }

    auto Insert(const K &key, const V &value) -> bool{
        std::scoped_lock<std::mutex> lock(mutex_);
        uint32_t hash_val = ExtendibleHashTable<K, V, KC>::Hash(key);
        uint32_t dir_idx = root_->HashToDirectoryIndex(hash_val);
        auto dir = root_->GetNode(dir_idx)->AsMut<ExtendibleHTableDirectoryNode>();

        if (dir == nullptr) {
            root_->SetNode(dir_idx, std::make_shared<ExtendibleHTableDirectoryNode>().get());
            dir = root_->GetNode(dir_idx)->AsMut<ExtendibleHTableDirectoryNode>();
        }
        // get bucket index
        uint32_t bucket_idx = dir->HashToBucketIndex(hash_val);
        auto bucket = dir->GetNode(bucket_idx)->AsMut<ExtendibleHTableBucketNode<K, V, KC>>();
        if (bucket == nullptr) {
            dir->SetNode(dir_idx, std::make_shared<ExtendibleHTableBucketNode<K, V, KC>>().get());
        }
        if (bucket->IsFull()) {
            if (!SplitBucket(dir, bucket, bucket_idx)) {
                return false;
            }
            // get bucket index
            uint32_t bucket_idx2 = dir->HashToBucketIndex(hash_val);
            auto bucket2 = dir->GetNode(bucket_idx2)->AsMut<ExtendibleHTableBucketNode<K, V, KC>>();

            if (bucket2->Lookup(key, const_cast<V &>(value), cmp_)) {
                return false;
            }
            return bucket2->Insert(key, value, cmp_);
        }
        return bucket->Insert(key, value, cmp_);
    }

    auto Remove(const K &key) -> bool{
        std::scoped_lock<std::mutex> lock(mutex_);
        uint32_t hash_val = ExtendibleHashTable<K, V, KC>::Hash(key);

        uint32_t dir_idx = root_->HashToDirectoryIndex(hash_val);
        auto dir = root_->GetNode(dir_idx)->AsMut<ExtendibleHTableDirectoryNode>();
        if (dir == nullptr) {
            return false;
        }
        // get bucket index
        uint32_t bucket_idx = dir->HashToBucketIndex(hash_val);
        auto bucket = dir->GetNode(bucket_idx)->AsMut<ExtendibleHTableBucketNode<K, V, KC>>();
        if (bucket == nullptr) {
            return false;
        }
        uint32_t value_idx = bucket->GetValueIndex(key, cmp_);
        if (value_idx == bucket->Size()) {
            return false;
        }
        bucket->RemoveAt(value_idx);
        if (bucket->Size() == 0) {
            MergeBucket(dir, bucket, bucket_idx);
        }
        return true;
    }
    auto Get(const K &key) -> std::optional<V> {
        std::scoped_lock<std::mutex> lock(mutex_);
        uint32_t hash_val = ExtendibleHashTable<K, V, KC>::Hash(key);

        uint32_t dir_idx = root_->HashToDirectoryIndex(hash_val);
        auto dir = root_->GetNode(dir_idx)->AsMut<ExtendibleHTableDirectoryNode>();
        if (dir == nullptr) {
            return std::nullopt;
        }
        // get bucket index
        uint32_t bucket_idx = dir->HashToBucketIndex(hash_val);
        auto bucket = dir->GetNode(bucket_idx)->AsMut<ExtendibleHTableBucketNode<K, V, KC>>();
        if (bucket == nullptr) {
            return std::nullopt;
        }
        uint32_t value_idx = bucket->GetValueIndex(key, cmp_);
        if (value_idx == bucket->Size()) {
            return std::nullopt;
        }
        return bucket->ValueAt(value_idx);
    }

    auto SplitBucket(ExtendibleHTableDirectoryNode *dir, ExtendibleHTableBucketNode<K,V,KC> *old_bucket,
                                                        uint32_t bucket_idx) -> bool {

        using BucketNode = ExtendibleHTableBucketNode<K, V, KC>;
        BucketNode* new_bucket = std::make_shared<BucketNode>().get();


        // Update local depth
        if (dir->GetLocalDepth(bucket_idx) == dir->GetMaxDepth()) {
            //    printf("Unable to grow!");
            return false;
        }
        if (dir->GetLocalDepth(bucket_idx) == dir->GetGlobalDepth()) {
            dir->IncrGlobalDepth();
        }
        uint32_t local_depth = dir->GetLocalDepth(bucket_idx) + 1;
        uint32_t new_bucket_idx = bucket_idx + (1U << (local_depth - 1));
        dir->SetNode(new_bucket_idx, new_bucket);
        dir->SetLocalDepth(bucket_idx, local_depth);
        dir->SetLocalDepth(new_bucket_idx, local_depth);

        std::vector<std::pair<K, V>> entries;
        // Redistribute entries
        for (uint32_t i = 0; i < old_bucket->Size(); i++) {
            auto key = old_bucket->KeyAt(i);
            uint32_t hash = ExtendibleHashTable<K, V, KC>::Hash(key);
            if (dir->HashToBucketIndex(hash) != bucket_idx) {
                new_bucket->PushBack(key, old_bucket->ValueAt(i), cmp_);
            } else {
                entries.push_back(old_bucket->EntryAt(i));
            }
        }
        // update old bucket size
        old_bucket->Init(bucket_max_size_);
        for (auto entry : entries) {
            old_bucket->PushBack(entry.first, entry.second, cmp_);
        }

        // Update directory mapping

        if (new_bucket->IsFull()) {
            SplitBucket(dir, new_bucket, new_bucket_idx);
        }
        if (old_bucket->IsFull()) {
            SplitBucket(dir, old_bucket, bucket_idx);
        }
        return true;
    }


    void MergeBucket(ExtendibleHTableDirectoryNode *dir, ExtendibleHTableBucketNode<K,V,KC> *old_bucket,
                                                        uint32_t bucket_idx) {
        // Fetch the other bucket page
        uint32_t local_depth = dir->GetLocalDepth(bucket_idx);
        if (local_depth <= 0) {
            return;
        }
        using BucketNode = ExtendibleHTableBucketNode<K, V, KC>;
        uint32_t low_bucket_idx = bucket_idx & ((1U << (local_depth - 1)) - 1);
        uint32_t high_bucket_idx = local_depth == 1 ? 1 : low_bucket_idx + (1U << (local_depth - 1));
        if (dir->GetLocalDepth(low_bucket_idx) != dir->GetLocalDepth(high_bucket_idx)) {
            return;
        }
        page_id_t other_bucket_id =
                bucket_idx == low_bucket_idx ? dir->GetBucketPageId(high_bucket_idx) : dir->GetBucketPageId(low_bucket_idx);
        auto other_bucket = dir->GetNode(other_bucket_id)->AsMut<BucketNode>();
        auto low_bucket  = bucket_idx == low_bucket_idx ? old_bucket : other_bucket;
        auto high_bucket  = bucket_idx == low_bucket_idx ? other_bucket : old_bucket;

        low_bucket->Merge(high_bucket);
        // Update directory mapping
        dir->DecrLocalDepth(low_bucket_idx);
        dir->DecrLocalDepth(high_bucket_idx);
        page_id_t high_bucket_id = dir->GetBucketPageId(high_bucket_idx);
        dir->SetNode(high_bucket_idx, nullptr);

        // originally less than global depth no need to reorg global

        bool need_decr_global_depth = local_depth == dir->GetGlobalDepth();
        //  uint32_t global_depth = dir->GetGlobalDepth();
        if (need_decr_global_depth) {
            dir->DecrGlobalDepth();  // this will not decr if conditions not met
        }
        dir->SetLocalDepth(low_bucket_idx, dir->GetLocalDepth(low_bucket_idx));
        if (dir->GetGlobalDepth() == 0) {
            return;
        }
        uint32_t new_high_bucket_idx = low_bucket_idx + (1U << (dir->GetLocalDepth(low_bucket_idx) - 1));
        auto new_high_bucket_node = dir->GetNode(new_high_bucket_idx);
        if (new_high_bucket_node != nullptr) {
            auto new_high_bucket = dir->GetNode(new_high_bucket_idx)->AsMut<BucketNode>();
            if (new_high_bucket->Size() == 0) {
                MergeBucket(dir, new_high_bucket, new_high_bucket_idx);
                return;
            }
        }
        if (low_bucket->Size() == 0) {
            MergeBucket(dir, low_bucket, low_bucket_idx);
        }
    }

private:
    std::mutex mutex_;
    ExtendibleHTableHeaderNode *root_;
    std::string name_;
    KC cmp_;
    HashFunction<K> hash_fn_;
    uint32_t bucket_max_size_;

};
}  // namespace eht
