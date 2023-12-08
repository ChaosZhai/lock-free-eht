//
// Created by Chaos Zhai on 12/8/23.
//

#include "node.h"

template <class T>
auto Node::As() -> const T * {
    return reinterpret_cast<const T *>(GetData());
}

template <class T>
auto Node::AsMut() -> T * {
    return reinterpret_cast<T *>(GetData());
}