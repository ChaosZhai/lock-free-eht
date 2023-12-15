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
        root_ = new InnerNode();
        auto new_root_data = new ExtendibleHTableHeaderNode();
        root_->SetData(reinterpret_cast<char*>(new_root_data));

    }

    auto Insert(const K &key, const V &value) -> bool{
        std::scoped_lock<std::mutex> lock(mutex_);
        uint32_t hash_val = ExtendibleHashTable<K, V, KC>::Hash(key);
        auto header = root_->AsMut<ExtendibleHTableHeaderNode>();
        uint32_t dir_idx = header->HashToDirectoryIndex(hash_val);
        auto* dir_node = reinterpret_cast<InnerNode*>(root_->GetNode(dir_idx));

        if (dir_node == nullptr) {
            auto new_dir = new ExtendibleHTableDirectoryNode();
            dir_node = new InnerNode();
            dir_node->SetData(reinterpret_cast<char*>(new_dir));
            root_->SetNode(dir_idx, dir_node);
        }
        auto dir = dir_node->AsMut<ExtendibleHTableDirectoryNode>();
        // get bucket index
        uint32_t bucket_idx = dir->HashToBucketIndex(hash_val);
        auto* bucket_node = reinterpret_cast<LeafNode<K, V, KC>*> (dir_node->GetNode(bucket_idx));
        if (bucket_node == nullptr) {
            auto new_bucket = new ExtendibleHTableBucket<K, V, KC>();
            bucket_node = new LeafNode<K, V, KC>();
            bucket_node->SetData(reinterpret_cast<char*>(new_bucket));
            dir_node->SetNode(bucket_idx, bucket_node);
        }
        auto bucket = bucket_node->template AsMut<ExtendibleHTableBucket<K, V, KC>>();
        if (bucket->IsFull()) {
            if (!SplitBucket(dir_node, bucket_node, bucket_idx)) {
                return false;
            }
            // get bucket index
            uint32_t bucket_idx2 = dir->HashToBucketIndex(hash_val);
            auto bucket2 = dir_node->GetNode(bucket_idx2)->AsMut<ExtendibleHTableBucket<K, V, KC>>();

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
        auto header = root_->AsMut<ExtendibleHTableHeaderNode>();
        uint32_t dir_idx = header->HashToDirectoryIndex(hash_val);
        auto* dir_node = reinterpret_cast<InnerNode*>(root_->GetNode(dir_idx));

        if (dir_node == nullptr) {
            return false;
        }
        auto dir = dir_node->AsMut<ExtendibleHTableDirectoryNode>();
        // get bucket index
        uint32_t bucket_idx = dir->HashToBucketIndex(hash_val);
        auto* bucket_node = reinterpret_cast<LeafNode<K, V, KC>*> (dir_node->GetNode(bucket_idx));
        if (bucket_node == nullptr) {
            return false;
        }
        auto bucket = bucket_node->template AsMut<ExtendibleHTableBucket<K, V, KC>>();
        uint32_t value_idx = bucket->GetValueIndex(key, cmp_);
        if (value_idx == bucket->Size()) {
            return false;
        }
        bucket->RemoveAt(value_idx);
        if (bucket->Size() == 0) {
            MergeBucket(dir_node, bucket_node, bucket_idx);
        }
        return true;
    }
    auto Get(const K &key) -> std::optional<V> {
        std::scoped_lock<std::mutex> lock(mutex_);
        uint32_t hash_val = ExtendibleHashTable<K, V, KC>::Hash(key);
        auto header = root_->AsMut<ExtendibleHTableHeaderNode>();
        uint32_t dir_idx = header->HashToDirectoryIndex(hash_val);
        auto* dir_node = reinterpret_cast<InnerNode*>(root_->GetNode(dir_idx));

        if (dir_node == nullptr) {
            return std::nullopt;
        }
        auto dir = dir_node->AsMut<ExtendibleHTableDirectoryNode>();
        // get bucket index
        uint32_t bucket_idx = dir->HashToBucketIndex(hash_val);
        auto bucket_node = dir_node->GetNode(bucket_idx);
        if (bucket_node == nullptr) {
           return std::nullopt;
        }
        auto bucket = bucket_node->AsMut<ExtendibleHTableBucket<K, V, KC>>();
        uint32_t value_idx = bucket->GetValueIndex(key, cmp_);
        if (value_idx == bucket->Size()) {
            return std::nullopt;
        }
        return bucket->ValueAt(value_idx);
    }

    auto SplitBucket(InnerNode *dir_node, LeafNode<K,V,KC> *old_bucket_node,
                                                        uint32_t bucket_idx) -> bool {

        auto dir = dir_node->AsMut<ExtendibleHTableDirectoryNode>();
        auto old_bucket = old_bucket_node->template AsMut<ExtendibleHTableBucket<K, V, KC>>();
        using Bucket = ExtendibleHTableBucket<K, V, KC>;
        auto* new_bucket = new Bucket();
        auto new_bucket_node = new LeafNode<K, V, KC>();
        new_bucket_node->SetData(reinterpret_cast<char*>(new_bucket));

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
        dir_node->SetNode(new_bucket_idx, new_bucket_node);
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
            SplitBucket(dir_node, new_bucket_node, new_bucket_idx);
        }
        if (old_bucket->IsFull()) {
            SplitBucket(dir_node, old_bucket_node, bucket_idx);
        }
        return true;
    }


    void MergeBucket(InnerNode *dir_node,LeafNode<K, V, KC> *old_bucket_node,
                                                        uint32_t bucket_idx) {
        // Fetch the other bucket node
        auto dir = dir_node->AsMut<ExtendibleHTableDirectoryNode>();
        auto old_bucket = old_bucket_node->template AsMut<ExtendibleHTableBucket<K, V, KC>>();
        uint32_t local_depth = dir->GetLocalDepth(bucket_idx);
        if (local_depth <= 0) {
            return;
        }
        uint32_t low_bucket_idx = bucket_idx & ((1U << (local_depth - 1)) - 1);
        uint32_t high_bucket_idx = local_depth == 1 ? 1 : low_bucket_idx + (1U << (local_depth - 1));
        if (dir->GetLocalDepth(low_bucket_idx) != dir->GetLocalDepth(high_bucket_idx)) {
            return;
        }
        node_id_t other_bucket_id =
                bucket_idx == low_bucket_idx ? dir->GetBucketId(high_bucket_idx) : dir->GetBucketId(low_bucket_idx);
        auto other_bucket_node = reinterpret_cast<LeafNode<K,V,KC>*>(dir_node->GetNode(other_bucket_id));
        auto other_bucket = old_bucket_node->template AsMut<ExtendibleHTableBucket<K, V, KC>>();
        auto low_bucket  = bucket_idx == low_bucket_idx ? old_bucket : other_bucket;
        auto low_bucket_node  = bucket_idx == low_bucket_idx ? old_bucket_node : other_bucket_node;
        auto high_bucket  = bucket_idx == low_bucket_idx ? other_bucket : old_bucket;

        low_bucket->Merge(high_bucket);
        // Update directory mapping
        dir->DecrLocalDepth(low_bucket_idx);
        dir->DecrLocalDepth(high_bucket_idx);
        {
            auto deleted_bucket_ptr = dir_node->GetNode(high_bucket_idx);
            free(deleted_bucket_ptr);
        }
        dir_node->SetNode(high_bucket_idx, nullptr);

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
        auto new_high_bucket_node = reinterpret_cast<LeafNode<K,V,KC>*>(dir_node->GetNode(new_high_bucket_idx));
        if (new_high_bucket_node != nullptr) {
            auto new_high_bucket = new_high_bucket_node->template AsMut<ExtendibleHTableBucket<K, V, KC>>();
            if (new_high_bucket->Size() == 0) {
                MergeBucket(dir_node, new_high_bucket_node, new_high_bucket_idx);
                return;
            }
        }
        if (low_bucket->Size() == 0) {
            MergeBucket(dir_node, low_bucket_node, low_bucket_idx);
        }
    }

private:
    std::mutex mutex_;
    InnerNode *root_;
    std::string name_;
    KC cmp_;
    HashFunction<K> hash_fn_;
    uint32_t bucket_max_size_;

};
}  // namespace eht
