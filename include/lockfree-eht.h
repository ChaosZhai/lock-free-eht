//
// Created by Chaos Zhai on 12/13/23.
//

#ifndef LOCKFREE_HASHTABLE_H
#define LOCKFREE_HASHTABLE_H

#include <atomic>
#include <cassert>
#include <cmath>

#include "../lib/hazardPointer/hazardPointer.h"
#include "lockfree_helpers/segment.h"
#include "lockfree_helpers/reverse.h"
#include "lockfree_helpers/node.h"
#include "lockfree_helpers/table_reclaimer.h"

namespace eht {

// The maximum bucket size equals to kSegmentSize^kMaxLevel, in this case the
// maximum bucket size is 64^4. If the load factor is 0.5, the maximum number of
// items that Hash Table contains is 64^4 * 0.5 = 2^23. You can adjust the
// following two values according to your memory size.

    const size_t kMaxBucketSize = static_cast<size_t>(pow(kSegmentSize, kMaxLevel));

// Hash Table can be stored 2^power_of_2_ * kLoadFactor items.
    const float kLoadFactor = 0.5;

    template<typename K, typename V, typename Hash = std::hash<K>>
    class LockFreeHashTable {
        static_assert(std::is_copy_constructible_v<K>, "K requires copy constructor");
        static_assert(std::is_copy_constructible_v<V>, "V requires copy constructor");
        friend TableReclaimer<K, V>;

    public:
        LockFreeHashTable() : power_of_2_(1), size_(0), hash_func_(Hash()) {
            // Initialize first bucket
            int level = 1;
            Segment *segments = segments_;  // Point to current segment.
            while (level <= kMaxLevel - 2) {
                Segment *sub_segments = NewSegments(level);
                segments[0].data.store(sub_segments, std::memory_order_release);
                segments = sub_segments;
                level++;
            }

            Bucket *buckets = NewBuckets();
            segments[0].data.store(buckets, std::memory_order_release);

            auto *head = new DummyNode(0);
            buckets[0].store(head, std::memory_order_release);
            head_ = head;
        }

        ~LockFreeHashTable() {
            Node *p = head_;
            while (p != nullptr) {
                Node *tmp = p;
                p = p->next.load(std::memory_order_acquire);
                tmp->Release();
            }
        }

        // Disable copy and move.
        LockFreeHashTable(const LockFreeHashTable &other) = delete;
        LockFreeHashTable(LockFreeHashTable &&other) = delete;
        LockFreeHashTable &operator=(const LockFreeHashTable &other) = delete;
        LockFreeHashTable &operator=(LockFreeHashTable &&other) = delete;

        bool Insert(const K &key, const V &value) {
            auto *new_node = new RegularNode(key, value, hash_func_);
            DummyNode *head = GetBucketHeadByHash(new_node->hash);
            return InsertRegularNode(head, new_node);
        }

        bool Remove(const K &key) {
            HashKey hash = hash_func_(key);
            DummyNode *head = GetBucketHeadByHash(hash);
            RegularNode delete_node(key, hash_func_);
            return DeleteNode(head, &delete_node);
        }

        bool Get(const K &key, V &value) {
            HashKey hash = hash_func_(key);
            DummyNode *head = GetBucketHeadByHash(hash);
            RegularNode find_node(key, hash_func_);
            return FindNode(head, &find_node, value);
        };

        size_t size() const { return size_.load(std::memory_order_relaxed); }

    private:
        size_t bucket_size() const {
            return 1 << power_of_2_.load(std::memory_order_relaxed);
        }

        // Initialize bucket recursively.
        DummyNode *InitializeBucket(BucketIndex bucket_index);

        // Get the head node of bucket, if bucket not exist then return nullptr or
        // return head.
        DummyNode *GetBucketHeadByIndex(BucketIndex bucket_index);

        // Get the head node of bucket, if bucket not exist then initialize it and
        // return head.
        DummyNode *GetBucketHeadByHash(HashKey hash) {
            BucketIndex bucket_index = (hash & (bucket_size() - 1));
            DummyNode *head = GetBucketHeadByIndex(bucket_index);
            if (head == nullptr) {
                head = InitializeBucket(bucket_index);
            }
            return head;
        }

        bool InsertRegularNode(DummyNode *head, RegularNode<K, V, Hash> *new_node);

        bool InsertDummyNode(DummyNode *parent_head, DummyNode *new_head, DummyNode **real_head);

        bool DeleteNode(DummyNode *head, Node *delete_node);

        bool FindNode(DummyNode *head, RegularNode<K, V, Hash> *find_node, V &value);

        // Traverse list begin with head until encounter nullptr or the first node
        // which is greater than or equals to the given search_node.
        bool SearchNode(DummyNode *head, Node *search_node, Node **prev_ptr,
                        Node **cur_ptr, HazardPointer &prev_hp,
                        HazardPointer &cur_hp);

        std::atomic<size_t> power_of_2_;   // Bucket size == 2^power_of_2_.
        std::atomic<size_t> size_;         // Item size.
        Hash hash_func_;                   // Hash function.
        Segment segments_[kSegmentSize];   // Top level segments.
        DummyNode *head_;                  // Head of linked list.
        static HazardPointerList global_hp_list_;
    };

    // global hazard pointer list.
    template<typename K, typename V, typename Hash>
    HazardPointerList LockFreeHashTable<K, V, Hash>::global_hp_list_;

    template<typename K, typename V, typename Hash>
    DummyNode *LockFreeHashTable<K, V, Hash>::InitializeBucket(BucketIndex bucket_index) {
        BucketIndex parent_index = GetBucketParent(bucket_index);
        DummyNode *parent_head = GetBucketHeadByIndex(parent_index);
        if (parent_head == nullptr) {
            parent_head = InitializeBucket(parent_index);
        }

        int level = 1;
        Segment *segments = segments_;  // Point to current segment.
        while (level++ <= kMaxLevel - 2) {
            Segment &cur_segment =
                    segments[(bucket_index / static_cast<SegmentIndex>(pow(
                            kSegmentSize, kMaxLevel - level + 1))) %
                             kSegmentSize];
            Segment *sub_segments = cur_segment.get_sub_segments();
            if (sub_segments == nullptr) {
                // Try allocate segments.
                sub_segments = NewSegments(level);
                void *expected = nullptr;
                if (!cur_segment.data.compare_exchange_strong(
                        expected, sub_segments, std::memory_order_release)) {
                    delete[] sub_segments;
                    sub_segments = static_cast<Segment *>(expected);
                }
            }
            segments = sub_segments;
        }

        Segment &cur_segment = segments[(bucket_index / kSegmentSize) % kSegmentSize];
        Bucket *buckets = cur_segment.get_sub_buckets();
        if (buckets == nullptr) {
            // Try to allocate buckets.
            void *expected = nullptr;
            buckets = NewBuckets();
            if (!cur_segment.data.compare_exchange_strong(expected, buckets,
                                                          std::memory_order_release)) {
                delete[] buckets;
                buckets = static_cast<Bucket *>(expected);
            }
        }

        Bucket &bucket = buckets[bucket_index % kSegmentSize];
        DummyNode *head = bucket.load(std::memory_order_consume);
        if (head == nullptr) {
            // Try to allocate dummy head.
            head = new DummyNode(bucket_index);
            DummyNode *real_head;  // If insert failed, real_head is the head of bucket.
            if (InsertDummyNode(parent_head, head, &real_head)) {
                // Dummy head must be inserted into the list before storing into bucket.
                bucket.store(head, std::memory_order_release);
            } else {
                delete head;
                head = real_head;
            }
        }
        return head;
    }

    template<typename K, typename V, typename Hash>
    DummyNode *LockFreeHashTable<K, V, Hash>::GetBucketHeadByIndex(BucketIndex bucket_index) {
        int level = 1;
        const Segment *segments = segments_;
        while (level++ <= kMaxLevel - 2) {
            segments =
                    segments[(bucket_index / static_cast<SegmentIndex>(pow(
                            kSegmentSize, kMaxLevel - level + 1))) %
                             kSegmentSize]
                            .get_sub_segments();
            if (nullptr == segments) return nullptr;
        }

        Bucket *buckets =
                segments[(bucket_index / kSegmentSize) % kSegmentSize].get_sub_buckets();
        if (buckets == nullptr) return nullptr;

        Bucket &bucket = buckets[bucket_index % kSegmentSize];
        return bucket.load(std::memory_order_consume);
    }

    template<typename K, typename V, typename Hash>
    bool LockFreeHashTable<K, V, Hash>::InsertDummyNode(DummyNode *parent_head, DummyNode *new_head,
                                                        DummyNode **real_head) {
        Node *prev;
        Node *cur;
        HazardPointer prev_hp, cur_hp;
        while (!prev->next.compare_exchange_weak(
                cur, new_head, std::memory_order_release, std::memory_order_relaxed)) {
            if (SearchNode(parent_head, new_head, &prev, &cur, prev_hp, cur_hp)) {
                // The head of bucket already insert into list.
                *real_head = dynamic_cast<DummyNode *>(cur);
                return false;
            }
            new_head->next.store(cur, std::memory_order_release);
        }
        return true;
    }

// Insert regular node into hash table, if its key is already exists in
// hash table then update it and return false else return true.
    template<typename K, typename V, typename Hash>
    bool LockFreeHashTable<K, V, Hash>::InsertRegularNode(DummyNode *head,
                                                          RegularNode<K, V, Hash> *new_node) {
        Node *prev;
        Node *cur;
        HazardPointer prev_hp, cur_hp;
        auto &reclaimer = TableReclaimer<K, V>::GetInstance(global_hp_list_);
        do {
            if (SearchNode(head, new_node, &prev, &cur, prev_hp, cur_hp)) {
                V *new_value = new_node->value.load(std::memory_order_consume);
                V *old_value = static_cast<RegularNode<K, V, Hash> *>(cur)->value.exchange(
                        new_value, std::memory_order_release);
                reclaimer.ReclaimLater(old_value,
                                       [](void *ptr) { delete static_cast<V *>(ptr); });
                new_node->value.store(nullptr, std::memory_order_release);
                delete new_node;
                return false;
            }
            new_node->next.store(cur, std::memory_order_release);
        } while (!prev->next.compare_exchange_weak(
                cur, new_node, std::memory_order_release, std::memory_order_relaxed));

        size_t size = size_.fetch_add(1, std::memory_order_relaxed) + 1;
        size_t power = power_of_2_.load(std::memory_order_relaxed);
        if (static_cast<float>(1 << power) * kLoadFactor < static_cast<float>(size)) {
            if (power_of_2_.compare_exchange_strong(power, power + 1,
                                                    std::memory_order_release)) {
                assert(bucket_size() <=
                       kMaxBucketSize);  // Out of memory or you can change the kMaxLevel
                // and kSegmentSize.
            }
        }
        return true;
    }

    template<typename K, typename V, typename Hash>
    bool LockFreeHashTable<K, V, Hash>::SearchNode(DummyNode *head, Node *search_node,
                                                   Node **prev_ptr, Node **cur_ptr,
                                                   HazardPointer &prev_hp,
                                                   HazardPointer &cur_hp) {
        auto &reclaimer = TableReclaimer<K, V>::GetInstance(global_hp_list_);
        try_again:
        Node *prev = head;
        Node *cur = prev->get_next();
        Node *next;
        while (true) {
            cur_hp.UnMark();
            cur_hp = HazardPointer(&reclaimer, cur);
            // Make sure prev is the predecessor of cur,
            // so that cur is properly marked as hazard.
            if (prev->get_next() != cur) goto try_again;

            if (cur == nullptr) {
                *prev_ptr = prev;
                *cur_ptr = cur;
                return false;
            }

            next = cur->get_next();
            if (is_marked_reference(next)) {
                if (!prev->next.compare_exchange_strong(cur,
                                                        get_unmarked_reference(next)))
                    goto try_again;

                reclaimer.ReclaimLater(cur, OnDeleteNode);
                reclaimer.ReclaimNoHazardPointer();
                size_.fetch_sub(1, std::memory_order_relaxed);
                cur = get_unmarked_reference(next);
            } else {
                if (prev->get_next() != cur) goto try_again;

                // Can not get copy_cur after above invocation,
                // because prev may not be the predecessor of cur at this point.
                if (GreaterOrEquals<K, V, Hash>(cur, search_node)) {
                    *prev_ptr = prev;
                    *cur_ptr = cur;
                    return Equals<K, V, Hash>(cur, search_node);
                }

                // Swap cur_hp and prev_hp.
                HazardPointer tmp = std::move(cur_hp);
                cur_hp = std::move(prev_hp);
                prev_hp = std::move(tmp);

                prev = cur;
                cur = next;
            }
        }

        // unreachable
        assert(false);
    }

    template<typename K, typename V, typename Hash>
    bool LockFreeHashTable<K, V, Hash>::DeleteNode(DummyNode *head,
                                                   Node *delete_node) {
        Node *prev;
        Node *cur;
        Node *next;
        HazardPointer prev_hp, cur_hp;
        // Logically delete cur by marking cur->next.
        while (!cur->next.compare_exchange_weak(next, get_marked_reference(next),
                                                std::memory_order_release,
                                                std::memory_order_relaxed)) {
            while (is_marked_reference(next)) {
                if (!SearchNode(head, delete_node, &prev, &cur, prev_hp, cur_hp)) {
                    return false;
                }
                next = cur->get_next();
            }
        }

        if (prev->next.compare_exchange_strong(cur, next,
                                               std::memory_order_release)) {
            size_.fetch_sub(1, std::memory_order_relaxed);
            auto &reclaimer = TableReclaimer<K, V>::GetInstance(global_hp_list_);
            reclaimer.ReclaimLater(cur, OnDeleteNode);
            reclaimer.ReclaimNoHazardPointer();
        } else {
            prev_hp.UnMark();
            cur_hp.UnMark();
            SearchNode(head, delete_node, &prev, &cur, prev_hp, cur_hp);
        }

        return true;
    }

    template<typename K, typename V, typename Hash>
    bool LockFreeHashTable<K, V, Hash>::FindNode(DummyNode *head,
                                                  RegularNode<K, V, Hash> *find_node,
                                                  V &value) {
        Node *prev;
        Node *cur;
        HazardPointer prev_hp, cur_hp;
        bool found = SearchNode(head, find_node, &prev, &cur, prev_hp, cur_hp);
        auto &reclaimer = TableReclaimer<K, V>::GetInstance(global_hp_list_);
        if (found) {
            V *value_ptr;
            V *temp;
            while (temp != value_ptr) {
                // When find and insert concurrently value may be deleted,
                // see InsertRegularNode, so value must be marked as hazard.
                value_ptr = static_cast<RegularNode<K, V, Hash> *>(cur)->value.load(
                        std::memory_order_consume);
                temp = value_ptr;
                value_ptr = static_cast<RegularNode<K, V, Hash> *>(cur)->value.load(
                        std::memory_order_consume);
            }

            reclaimer.ReclaimNoHazardPointer();
            value = *value_ptr;
        }
        return found;
    }

#endif  // LOCKFREE_HASHTABLE_H

} // namespace eht