//
// Created by Yicheng Zhang on 12/12/23.
//

/**
 * Directory page format:
 *  --------------------------------------------------------------------------------------
 * | MaxDepth (4) | GlobalDepth (4) | LocalDepths (512) | BucketPageIds(2048) |
 * Free(1528)
 *  --------------------------------------------------------------------------------------
 */

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>
#include "../../lib/node/inner-node.hpp"
#ifndef LOCK_FREE_EHT_HTABLE_DIRECTORY_H
#define LOCK_FREE_EHT_HTABLE_DIRECTORY_H
namespace eht {

/**
 * HTABLE_DIRECTORY_ARRAY_SIZE is the number of page_ids that can fit in the
 * directory page of an extendible hash index. This is 512 because the directory
 * array must grow in powers of 2, and 1024 page_ids leaves zero room for
 * storage of the other member variables.
 */


    static constexpr uint64_t HTABLE_DIRECTORY_ARRAY_SIZE =
            1 << HTABLE_DIRECTORY_MAX_DEPTH;

/**
 * Directory Page for extendible hash table.
 */
    class ExtendibleHTableDirectoryNode {
    public:
        explicit ExtendibleHTableDirectoryNode(uint32_t max_depth = HTABLE_DIRECTORY_MAX_DEPTH) {
            max_depth_ = max_depth;
            global_depth_ = 0; // Start with global depth of 0
            for (uint32_t i = 0; i < HTABLE_DIRECTORY_ARRAY_SIZE; i++) {
                local_depths_[i] = 0;
                bucket_page_ids_[i] = INVALID_PAGE_ID;
            }
        }



        /**
         * Get the bucket index that the key is hashed to
         *
         * @param hash the hash of the key
         * @return bucket index current key is hashed to
         */
        [[nodiscard]] auto HashToBucketIndex(uint32_t hash) const -> uint32_t {
            return hash & GetGlobalDepthMask();
        }

        /**
         * Lookup a bucket page using a directory index
         *
         * @param bucket_idx the index in the directory to lookup
         * @return bucket page_id corresponding to bucket_idx
         */
        [[nodiscard]] auto GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
            if (bucket_idx >= (1U << global_depth_)) {
                throw std::out_of_range("Bucket index out of bound");
            }
            return bucket_page_ids_[bucket_idx];
        }

        /**
         * Updates the directory index using a bucket index and page_id
         *
         * @param bucket_idx directory index at which to insert page_id
         * @param bucket_page_id page_id to insert
         */
        void SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
            if (bucket_idx >= (1U << global_depth_)) {
                throw std::out_of_range("Bucket index out of bound");
            }
            bucket_page_ids_[bucket_idx] = bucket_page_id;
        }

        /**
         * Gets the split image of an index
         *
         * @param bucket_idx the directory index for which to find the split image
         * @return the directory index of the split image
         **/
        [[nodiscard]] auto GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
            return bucket_idx ^ GetGlobalDepthMask();
        }

        /**
         * GetGlobalDepthMask - returns a mask of global_depth 1's and the rest 0's.
         *
         * In Extendible Hashing we map a key to a directory index
         * using the following hash + mask function.
         *
         * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
         *
         * where GLOBAL_DEPTH_MASK is a mask with exactly GLOBAL_DEPTH 1's from LSB
         * upwards.  For example, global depth 3 corresponds to 0x00000007 in a 32-bit
         * representation.
         *
         * @return mask of global_depth 1's and the rest 0's (with 1's from LSB
         * upwards)
         */
        [[nodiscard]] auto GetGlobalDepthMask() const -> uint32_t {
            return (1U << global_depth_) - 1;
        }

        /**
         * GetLocalDepthMask - same as global depth mask, except it
         * uses the local depth of the bucket located at bucket_idx
         *
         * @param bucket_idx the index to use for looking up local depth
         * @return mask of local 1's and the rest 0's (with 1's from LSB upwards)
         */
        [[nodiscard]] auto GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
            return (1U << global_depth_) - 1;
        }

        /**
         * Get the global depth of the hash table directory
         *
         * @return the global depth of the directory
         */
        [[nodiscard]] auto GetGlobalDepth() const -> uint32_t { return global_depth_; }

        [[nodiscard]] auto GetMaxDepth() const -> uint32_t { return max_depth_; }

        /**
         * Increment the global depth of the directory
         */
        void IncrGlobalDepth() {
            for (uint32_t i = 0; i < (1U << global_depth_); i++) {
                bucket_page_ids_[i + (1U << global_depth_)] = bucket_page_ids_[i];
                local_depths_[i + (1U << global_depth_)] = local_depths_[i];
            }
            global_depth_++;
        }

        /**
         * Decrement the global depth of the directory
         */
        void DecrGlobalDepth() {
            if (global_depth_ == 0) {
                return;
            }
            for (uint32_t i = 0; i < (1U << global_depth_); i++) {
                if (local_depths_[i] >= global_depth_) {
                    return;
                }
            }
            global_depth_--;
        }

        /**
         * @return true if the directory can be shrunk
         */
        auto CanShrink() -> bool {
            if (global_depth_ == 0) {
                return false;
            }
            for (uint32_t i = 0; i < (1U << global_depth_); i++) {
                if (local_depths_[i] >= global_depth_) {
                    return false;
                }
            }
            return true;
        }

        /**
         * @return the current directory size
         */
        [[nodiscard]] auto Size() const -> uint32_t { return 1U << global_depth_; }

        /**
         * @return the max directory size
         */
        [[nodiscard]] auto MaxSize() const -> uint32_t { return 1U << max_depth_; }

        /**
         * Gets the local depth of the bucket at bucket_idx
         *
         * @param bucket_idx the bucket index to lookup
         * @return the local depth of the bucket at bucket_idx
         */
        [[nodiscard]] auto GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
            return local_depths_[bucket_idx];
        }

        /**
         * Set the local depth of the bucket at bucket_idx to local_depth
         *
         * @param bucket_idx bucket index to update
         * @param local_depth new local depth
         */
        void SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
            local_depths_[bucket_idx] = local_depth;
            if (local_depth != global_depth_) {
                uint8_t diff_bits = global_depth_ - local_depth;
                uint32_t cnt = 1U << diff_bits;
                uint32_t gap = 1U << local_depth;
                for (uint32_t i = 0; i < cnt; i++) {
                    bucket_page_ids_[bucket_idx + gap] = bucket_page_ids_[bucket_idx];
                    local_depths_[bucket_idx + gap] = local_depth;
                }
            }
        }

        /**
         * Increment the local depth of the bucket at bucket_idx
         * @param bucket_idx bucket index to increment
         */
        void IncrLocalDepth(uint32_t bucket_idx) {
            local_depths_[bucket_idx]++;
            if (local_depths_[bucket_idx] > global_depth_) {
                IncrGlobalDepth();
            }
        }

        /**
         * Decrement the local depth of the bucket at bucket_idx
         * @param bucket_idx bucket index to decrement
         */
        void DecrLocalDepth(uint32_t bucket_idx) {
            if (local_depths_[bucket_idx] > 0) {
                local_depths_[bucket_idx]--;
            }
        }

        /**
         * VerifyIntegrity
         *
         * Verify the following invariants:
         * (1) All LD <= GD.
         * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.
         * (3) The LD is the same at each index with the same bucket_page_id
         */
        void VerifyIntegrity() const {
            // build maps of {bucket_page_id : pointer_count} and {bucket_page_id :
            // local_depth}
            std::unordered_map<page_id_t, uint32_t> page_id_to_count =
                    std::unordered_map<page_id_t, uint32_t>();
            std::unordered_map<page_id_t, uint32_t> page_id_to_ld =
                    std::unordered_map<page_id_t, uint32_t>();

            // Verify: (3) The LD is the same at each index with the same bucket_page_id
            for (uint32_t curr_idx = 0; curr_idx < Size(); curr_idx++) {
                page_id_t curr_page_id = bucket_page_ids_[curr_idx];
                uint32_t curr_ld = local_depths_[curr_idx];

                // Verify: (1) All LD <= GD.
                ASSERT(curr_ld <= global_depth_,
                              "there exists a local depth greater than the global depth");

                ++page_id_to_count[curr_page_id];

                if (page_id_to_ld.count(curr_page_id) > 0 &&
                    curr_ld != page_id_to_ld[curr_page_id]) {
                    uint32_t old_ld = page_id_to_ld[curr_page_id];
                    printf("Verify Integrity: curr_local_depth: %u, old_local_depth %u, "
                             "for page_id: %u",
                             curr_ld, old_ld, curr_page_id);
                    PrintDirectory();
                    ASSERT(curr_ld == page_id_to_ld[curr_page_id],
                                  "local depth is not the same at each index with same "
                                  "bucket page id");
                } else {
                    page_id_to_ld[curr_page_id] = curr_ld;
                }
            }

            // Verify: (2) Each bucket has precisely 2^(GD - LD) pointers pointing to
            // it.
            auto it = page_id_to_count.begin();
            while (it != page_id_to_count.end()) {
                page_id_t curr_page_id = it->first;
                uint32_t curr_count = it->second;
                uint32_t curr_ld = page_id_to_ld[curr_page_id];
                uint32_t required_count = 0x1 << (global_depth_ - curr_ld);
                it++;
            }
        }

        /**
         * Prints the current directory
         */
        void PrintDirectory() const {
            printf("======== DIRECTORY (global_depth_: %u) ========", global_depth_);
            printf("| bucket_idx | page_id | local_depth |");
            for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_);
                 idx++) {
                printf("|    %u    |    %u    |    %u    |", idx,
                          bucket_page_ids_[idx], local_depths_[idx]);
            }
            printf("================ END DIRECTORY ================");
        }

    private:
        uint32_t max_depth_;
        uint32_t global_depth_;
        uint8_t local_depths_[HTABLE_DIRECTORY_ARRAY_SIZE]{};
        page_id_t bucket_page_ids_[HTABLE_DIRECTORY_ARRAY_SIZE]{};
    };

    static_assert(sizeof(page_id_t) == 4);

} // namespace eht

#endif // LOCK_FREE_EHT_HTABLE_DIRECTORY_H
