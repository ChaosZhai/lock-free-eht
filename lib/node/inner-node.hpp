//
// Created by Chaos Zhai on 12/8/23.
//
#include "node.h"
#define DEPTH 9
#define HEADER_NODE_SIZE 1 << DEPTH
namespace eht {
#pragma once

    class InnerNode : public Node {
    public:
        [[nodiscard]] Node *GetNode(uint32_t index) const {
            if (index >= HEADER_NODE_SIZE) {
               return nullptr;
            }
            return array_[index];
        }

        void SetNode(uint32_t index, Node *node) {
            array_[index] = node;
        }

    private:
        Node *array_[HEADER_NODE_SIZE];
    };
}  // namespace eht