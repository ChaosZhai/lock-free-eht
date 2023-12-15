//
// Created by Yicheng Zhang on 12/12/23.
//
#pragma once
static constexpr int INVALID_NODE_ID = -1; // invalid node id

static constexpr uint64_t HTABLE_HEADER_ARRAY_SIZE = 1 << HTABLE_HEADER_MAX_DEPTH;
using node_id_t = int32_t; // node id type

#define MappingType std::pair<KeyType, ValueType>

