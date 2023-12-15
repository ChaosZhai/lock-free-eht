//
// Created by Chaos Zhai on 12/13/23.
//
#pragma once

#ifndef LOCK_FREE_EHT_RECLAIMER_H
#define LOCK_FREE_EHT_RECLAIMER_H

#include <atomic>
#include <cassert>
#include <functional>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "internalHazardPointer.h"

namespace eht {

    class HazardPointer;

    class Reclaimer {
        friend class HazardPointer;

    private:
        // The max number of reclaim node in reclaim list.
        static const int maxNodes = 4;
        static const int HP_INDEX_NULL = -1;

    public:
        // delete copy and move constructors
        Reclaimer(const Reclaimer &) = delete;
        Reclaimer(Reclaimer &&) = delete;
        Reclaimer &operator=(const Reclaimer &) = delete;
        Reclaimer &operator=(Reclaimer &&) = delete;

        // Mark ptr as hazard.
        int MarkHazard(void *ptr);

        void UnMarkHazard(int index) {
            if (index == HP_INDEX_NULL) return;

            assert(index >= 0 && index < hp_list_.size());
            hp_list_[index]->ptr.store(nullptr, std::memory_order_release);
        }

        // Get ptr that marked as hazard at the index of hazard_pointers_ array.
        void *GetHazardPtr(int index) {
            if (index == HP_INDEX_NULL) return nullptr;

            assert(index >= 0 && index < hp_list_.size());
            return hp_list_[index]->ptr.load(std::memory_order_relaxed);
        }

        // If ptr is hazard then reclaim it later.
        // put ptr into reclaim map.
        void ReclaimLater(void *const ptr, std::function<void(void *)> &&func) {
            ReclaimNode *new_node = reclaim_pool_.Pop();
            new_node->ptr = ptr;
            new_node->delete_func = std::move(func);
            reclaim_map_.insert(std::make_pair(ptr, new_node));
        }

        // Try to reclaim all no hazard pointers.
        void ReclaimNoHazardPointer();

    protected:
        explicit Reclaimer(HazardPointerList &hp_list);

        ~Reclaimer();

    private:
        // Check if the ptr is hazard.
        bool Hazard(void *ptr);

        /**
         * Try to acquire a new hazard pointer from global hazard pointer list.
         * Or allocate a new hazard pointer.
         * Push the new hazard pointer into hazard pointer list.
         */
        void TryAcquireHazardPointer();

        struct ReclaimNode {
            ReclaimNode() : ptr(nullptr), next(nullptr), delete_func(nullptr) {}

            ~ReclaimNode() = default;

            void *ptr;
            ReclaimNode *next;
            std::function<void(void *)> delete_func;
        };

        // Reuse ReclaimNode
        // dll to store ReclaimNodes
        struct ReclaimPool {
            ReclaimPool() : head(new ReclaimNode()) {}

            ~ReclaimPool() {
                ReclaimNode *temp;
                while (head) {
                    temp = head;
                    head = head->next;
                    delete temp;
                }
            }

            void Push(ReclaimNode *node) {
                node->next = head;
                head = node;
            }

            ReclaimNode *Pop() {
                if (nullptr == head->next) {
                    return new ReclaimNode();
                }
                ReclaimNode *temp = head;
                head = head->next;
                temp->next = nullptr;
                return temp;
            }

            ReclaimNode *head;
        };

        std::vector<InternalHazardPointer *> hp_list_;
        std::unordered_map<void *, ReclaimNode *> reclaim_map_;
        ReclaimPool reclaim_pool_;
        HazardPointerList &global_hp_list_;
    };

#endif //LOCK_FREE_EHT_RECLAIMER_H

} // namespace eht