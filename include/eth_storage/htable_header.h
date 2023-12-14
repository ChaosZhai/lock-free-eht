//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.h
//
//
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group, Yicheng Zhang
//
//===----------------------------------------------------------------------===//

/**
 * Header page format:
 *  ---------------------------------------------------
 * | DirectoryPageIds(2048) | MaxDepth (4) | Free(2044)
 *  ---------------------------------------------------
 */

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include "../common.h"
#include "../../lib/node/inner-node.hpp"

#ifndef LOCK_FREE_EHT_HTABLE_HEADER_H
#define LOCK_FREE_EHT_HTABLE_HEADER_H


namespace eht {
    class ExtendibleHTableHeaderNode  {
    public:

        ExtendibleHTableHeaderNode(uint32_t max_depth = HTABLE_HEADER_MAX_DEPTH) {
            max_depth_ = max_depth;
            for (int &directory_page_id: directory_page_ids_) {
                directory_page_id = INVALID_PAGE_ID;
            }
        }

        /**
         * Get the directory index that the key is hashed to
         *
         * @param hash the hash of the key
         * @return directory index the key is hashed to
         */
        [[nodiscard]] auto HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
            return hash >> (32 - max_depth_) & ((1 << max_depth_) - 1);
        }

        /**
         * Get the directory page id at an index
         *
         * @param directory_idx index in the directory page id array
         * @return directory page_id at index
         */
        [[nodiscard]] auto GetDirectoryPageId(uint32_t directory_idx) const -> page_id_t {
            if (directory_idx >= MaxSize()) {
                throw std::out_of_range("Directory index out of bound");
            }
            return directory_page_ids_[directory_idx];
        }

        /**
         * @brief Set the directory page id at an index
         *
         * @param directory_idx index in the directory page id array
         * @param directory_page_id page id of the directory
         */
        void SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
            if (directory_idx >= MaxSize()) {
                throw std::out_of_range("Directory index out of bound");
            }
            directory_page_ids_[directory_idx] = directory_page_id;
        }


        /**
         * @brief Get the maximum number of directory page ids the header page could handle
         */
        [[nodiscard]] auto MaxSize() const -> uint32_t { return 1 << max_depth_; }

        void PrintHeader() const {
            LOG_DEBUG("======== HEADER (max_depth_: %u) ========", max_depth_);
            LOG_DEBUG("| directory_idx | page_id |");
            for (uint32_t idx = 0; idx < static_cast<uint32_t>(1 << max_depth_); idx++) {
                LOG_DEBUG("|    %u    |    %u    |", idx, directory_page_ids_[idx]);
            }
            LOG_DEBUG("======== END HEADER ========");
        }

    private:
        page_id_t directory_page_ids_[HTABLE_HEADER_ARRAY_SIZE] = {0};
        uint32_t max_depth_;
    };

} // namespace eht



#endif //LOCK_FREE_EHT_HTABLE_HEADER_H
