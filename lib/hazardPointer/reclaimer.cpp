//
// Created by Chaos Zhai on 12/14/23.
//

#include "reclaimer.h"
namespace eht {

    int Reclaimer::MarkHazard(void* ptr) {
        if (nullptr == ptr) return HP_INDEX_NULL;

        // find a idle hazard pointer
        for (int i = 0; i < hp_list_.size(); ++i) {
            InternalHazardPointer* hp = hp_list_[i];
            if (hp->ptr.load(std::memory_order_relaxed) == nullptr) {
                hp->ptr.store(ptr, std::memory_order_release);
                return i;
            }
        }

        // acquire a new hazard pointer
        TryAcquireHazardPointer();
        int index = static_cast<int>(hp_list_.size()) - 1;
        hp_list_[index]->ptr.store(ptr, std::memory_order_release);
        return index;
    }

    void Reclaimer::ReclaimNoHazardPointer() {
        if (reclaim_map_.size() < maxNodes * global_hp_list_.get_size()) {
            return;
        }

        // Used to speed up the inspection of the ptr.
        std::unordered_set<void *> not_allow_delete_set;
        std::atomic<InternalHazardPointer *> &head = global_hp_list_.head;
        InternalHazardPointer *p = head.load(std::memory_order_acquire);
        while (p) {
            void *const ptr = p->ptr.load(std::memory_order_consume);
            if (nullptr != ptr) {
                not_allow_delete_set.insert(ptr);
            }
            p = p->next.load(std::memory_order_acquire);
        }

        for (auto it = reclaim_map_.begin(); it != reclaim_map_.end();) {
            if (not_allow_delete_set.find(it->first) == not_allow_delete_set.end()) {
                ReclaimNode *node = it->second;
                node->delete_func(node->ptr);
                reclaim_pool_.Push(node);
                it = reclaim_map_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void Reclaimer::TryAcquireHazardPointer() {
        std::atomic<InternalHazardPointer *> &head = global_hp_list_.head;
        InternalHazardPointer *p = head.load(std::memory_order_acquire);
        InternalHazardPointer *hp = nullptr;

        while (p) {
            // Try to get the idle hazard pointer that's previously false.
            // and set flag to true
            if (!p->flag.test_and_set()) {
                hp = p;
                break;
            }
            p = p->next.load(std::memory_order_acquire);
        }

        if (hp == nullptr) {
            // No idle hazard pointer, allocate new one.
            auto *new_head = new InternalHazardPointer();
            new_head->flag.test_and_set();
            hp = new_head;
            global_hp_list_.size.fetch_add(1, std::memory_order_release);
            InternalHazardPointer *old_head = head.load(std::memory_order_acquire);
            new_head->next = old_head;
            while (!head.compare_exchange_weak(old_head, new_head,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
                new_head->next = old_head;

            }
        }
        hp_list_.push_back(hp);
    }

    bool Reclaimer::Hazard(void *const ptr) {
        std::atomic<InternalHazardPointer *> &head = global_hp_list_.head;
        InternalHazardPointer *p = head.load(std::memory_order_acquire);
        while (p) {
            if (p->ptr.load(std::memory_order_consume) == ptr) {
                return true;
            }
            p = p->next.load(std::memory_order_acquire);
        }

        return false;
    }

    Reclaimer::~Reclaimer() {
        // The Reclaimer destruct when the thread exit
        // 1.Hand over the hazard pointer
        for (auto &i: hp_list_) {
            // If assert failed, you should make sure no pointer is marked as hazard
            // before thread exit
            assert(nullptr == i->ptr.load(std::memory_order_relaxed));
            i->flag.clear();
        }

        // 2.Make sure reclaim all no hazard pointers
        for (auto it = reclaim_map_.begin(); it != reclaim_map_.end();) {
            // Wait until pointer is no hazard
            while (Hazard(it->first)) {
                std::this_thread::yield();
            }

            ReclaimNode *node = it->second;
            node->delete_func(node->ptr);
            delete node;
            it = reclaim_map_.erase(it);
        }
    }

    Reclaimer::Reclaimer(HazardPointerList &hp_list) : global_hp_list_(hp_list) {}

}