/**
 * Header node format:
 *  ---------------------------------------------------
 * | DirectorynodeIds(2048) | MaxDepth (4) | Free(2044)
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

        explicit ExtendibleHTableHeaderNode(uint32_t max_depth = HTABLE_HEADER_MAX_DEPTH) {
            max_depth_ = max_depth;
            for (int &directory_node_id: directory_node_ids_) {
                directory_node_id = INVALID_NODE_ID;
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
         * @brief Get the maximum number of directory node ids the header node could handle
         */
        [[nodiscard]] auto MaxSize() const -> uint32_t { return 1 << max_depth_; }

        void PrintHeader() const {
            printf("======== HEADER (max_depth_: %u) ========", max_depth_);
            printf("| directory_idx | node_id |");
            for (uint32_t idx = 0; idx < static_cast<uint32_t>(1 << max_depth_); idx++) {
                printf("|    %u    |    %u    |", idx, directory_node_ids_[idx]);
            }
            printf("======== END HEADER ========");
        }

    private:
        node_id_t directory_node_ids_[HTABLE_HEADER_ARRAY_SIZE] = {0};
        uint32_t max_depth_;
    };

} // namespace eht

#endif //LOCK_FREE_EHT_HTABLE_HEADER_H
