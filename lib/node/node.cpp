//
// Created by Chaos Zhai on 12/8/23.
//

#include "node.h"
namespace eht {
//    template<class T>
//    auto Node::As() -> const T * {
//        return reinterpret_cast<const T *>(GetData());
//    }

//    template<class T>
//    auto Node::AsMut() -> T * {
//        return reinterpret_cast<T *>(GetData());
//    }

    auto Node::GetDirIndex(uint32_t hash_val, bool msb) -> uint32_t {
        uint32_t mask = 1 << (32 - DEPTH);
        if (msb) {
            return (hash_val >> (32 - DEPTH)) & mask;
        }
        return hash_val & mask;
    }
}  // namespace eht