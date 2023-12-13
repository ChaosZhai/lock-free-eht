//
// Created by Chaos Zhai on 12/8/23.
//
#include "node.h"
#define DEPTH 8
#define HEADER_NODE_SIZE 1 << DEPTH

class InnerNode : public Node {
public:
    [[nodiscard]] Node *GetNode(uint32_t index) const {
        return array_[index];
    }

    void SetNode(uint32_t index, Node *node) {
        array_[index] = node;
    }

private:
    Node* array_[HEADER_NODE_SIZE];
    uint32_t local_depth_[HEADER_NODE_SIZE];
    uint32_t global_depth_;
};