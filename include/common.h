//
// Created by Yicheng Zhang on 12/12/23.
//
#pragma once
static constexpr int INVALID_PAGE_ID = -1; // invalid page id
static constexpr uint64_t HTABLE_HEADER_PAGE_METADATA_SIZE = sizeof(uint32_t);

static constexpr uint64_t HTABLE_HEADER_ARRAY_SIZE = 1
                                                     << HTABLE_HEADER_MAX_DEPTH;
static constexpr uint64_t BUCKET_SIZE = 4096;
using page_id_t = int32_t; // page id type

#define MappingType std::pair<KeyType, ValueType>

#define ASSERT(expr, message) assert((expr) && (message))
