//
// Created by Chaos Zhai on 12/8/23.
//

#include <shared_mutex>

#define NODE_SIZE 4000
#define DEPTH 8
#pragma once
namespace eht {
    class Node {
    public:
        Node() {
            data_ = new char[NODE_SIZE];
            ResetMemory();
        }

        ~Node() {
            delete data_;
        }

        char *GetData() {
            return data_;
        }

        inline void WLock() { mutex_.lock(); }

        inline void WUnlock() { mutex_.unlock(); }

        inline void RLock() { mutex_.lock_shared(); }

        inline void RUnlock() { mutex_.unlock_shared(); }

        template<class T>
        const T *As();

        template<class T>
        T *AsMut();

        static uint32_t GetDirIndex(uint32_t hash_val, bool msb);

    private:
        /** Zeroes out the data. */
        inline void ResetMemory() { memset(data_, 0, NODE_SIZE); }

        char *data_;
        std::shared_mutex mutex_;
    };
}  // namespace eht