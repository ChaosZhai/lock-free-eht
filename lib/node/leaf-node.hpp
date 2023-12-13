//
// Created by Chaos Zhai on 12/9/23.
//
#include <cassert>

#define LEAF_NODE_SIZE 1 << DEPTH
namespace eht {
#pragma once
template<typename K, typename V, typename KC>
class LeafNode : public Node {
public:
    LeafNode() : Node() {
        size_ = 0;

    }

    [[nodiscard]] auto GetSize() const -> uint32_t {
        return size_;
    }

    void SetSize(uint32_t size) {
        assert(size <= max_size_);
        size_ = size;
    }

    [[nodiscard]] auto GetMaxSize() const -> uint32_t {
        return max_size_;
    }

    auto Get(const K& key) const -> std::optional<V> {
        for (int i = 0; i < size_; ++i) {
            if (array_[i].first == key) {
                return array_[i].second;
            }
        }
        return std::nullopt;
    }

    auto Insert(const K& key, const V& value) -> bool {
        if (size_ == max_size_) {
            return false;
        }
        if (Get(key)) {
            return false;
        }
        array_[size_++] = {key, value};
        return true;
    }

    auto Remove(const K& key) -> bool {
        for (int i = 0; i < size_; ++i) {
            if (array_[i].first == key) {
                std::swap(array_[i], array_[--size_]);
                return true;
            }
        }
        return false;
    }

private:
    uint32_t size_;
    uint32_t max_size_ = LEAF_NODE_SIZE;
    using KV = std::pair<K, V>;
    KV array_[LEAF_NODE_SIZE];
};
} // namespace eht