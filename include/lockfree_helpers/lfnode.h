//
// Created by Chaos Zhai on 12/13/23.
//
#pragma once
#include <atomic>
#include <utility>
#include "reverse.h"

namespace eht {

    using HashKey = size_t;
    using BucketIndex = size_t;
    const size_t mask = 0x8000000000000000;

    class LFNode {
    public:
        LFNode(HashKey hash_, bool dummy)
                : hash(hash_),
                  reverse_hash(dummy ? DummyKey(hash) : RegularKey(hash)),
                  next(nullptr) {}

        virtual void Release() = 0;

        virtual ~LFNode() = default;

        static HashKey RegularKey(HashKey hash) {
            return Reverse(hash | mask);
        }

        static HashKey DummyKey(HashKey hash) { return Reverse(hash); }

        virtual bool IsDummy() const { return (reverse_hash & 0x1) == 0; }

        LFNode *get_next() const { return next.load(std::memory_order_acquire); }

        const HashKey hash;
        const HashKey reverse_hash;
        std::atomic<LFNode *> next;
    };

    // Head node of bucket
    class DummyNode : public LFNode {
    public:
        explicit DummyNode(BucketIndex bucket_index) : LFNode(bucket_index, true) {}

        ~DummyNode() override = default;

        void Release() override { delete this; }

        bool IsDummy() const override { return true; }
    };


    template<typename K, typename V, typename Hash>
    class RegularNode : public LFNode {
    public:
        RegularNode(const K &key_, const V &value_, const Hash &hash_func)
                : LFNode(hash_func(key_), false), key(key_), value(new V(value_)) {}

        RegularNode(const K &key_, V &&value_, const Hash &hash_func)
                : LFNode(hash_func(key_), false),
                  key(key_),
                  value(new V(std::move(value_))) {}

        RegularNode(K &&key_, const V &value_, const Hash &hash_func)
                : LFNode(hash_func(key_), false),
                  key(std::move(key_)),
                  value(new V(value_)) {}

        RegularNode(K &&key_, V &&value_, const Hash &hash_func)
                : LFNode(hash_func(key_), false),
                  key(std::move(key_)),
                  value(new V(std::move(value_))) {}

        RegularNode(const K &key_, const Hash &hash_func)
                : LFNode(hash_func(key_), false), key(key_), value(nullptr) {}

        ~RegularNode() override {
            V *ptr = value.load(std::memory_order_consume);
            delete ptr;  // If update a node, value of this node is nullptr.
        }

        void Release() override { delete this; }

        bool IsDummy() const override { return false; }

        const K key;
        std::atomic<V *> value;
    };

    /*********************
     * Node Helper Functions
     *********************/

    template<typename K, typename V, typename Hash>
    bool LessRN(RegularNode<K, V, Hash> *node1, RegularNode<K, V, Hash> *node2) {
        return node1->key < node2->key;
    }
    // Compare two nodes according to their reverse_hash and the key.

    template<typename K, typename V, typename Hash>
    bool Less(LFNode *node1, LFNode *node2) {
        if (node1->reverse_hash != node2->reverse_hash) {
            return node1->reverse_hash < node2->reverse_hash;
        }

        if (node1->IsDummy() || node2->IsDummy()) {
            // When initialize bucket concurrently, that could happen.
            return false;
        }

        auto *regular_node1 = dynamic_cast<RegularNode<K, V, Hash> *>(node1);
        auto *regular_node2 = dynamic_cast<RegularNode<K, V, Hash> *>(node2);
        return LessRN<K, V, Hash>(regular_node1, regular_node2);
    }

    template<typename K, typename V, typename Hash>
    bool GreaterOrEquals(LFNode *node1, LFNode *node2) {
        return !(Less<K, V, Hash>(node1, node2));
    }

    template<typename K, typename V, typename Hash>
    bool Equals(LFNode *node1, LFNode *node2) {
        return !Less<K, V, Hash>(node1, node2)
                && !Less<K, V, Hash>(node2,node1);
    }

    bool is_marked_reference(LFNode *next);

    LFNode *get_marked_reference(LFNode *next);

    LFNode *get_unmarked_reference(LFNode *next);

    static void OnDeleteNode(void *ptr) { delete static_cast<LFNode *>(ptr); }

}  // namespace eht