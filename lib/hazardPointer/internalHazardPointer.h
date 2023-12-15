//
// Created by Chaos Zhai on 12/13/23.
//
#pragma once
#include <atomic>

namespace eht {

/**
 * InternalHazardPointer is used to store the hazard pointer.
 * It is a node of HazardPointerList.
 */
    class InternalHazardPointer {
    public:
        InternalHazardPointer() : ptr(nullptr), next(nullptr) {}

        ~InternalHazardPointer() = default;

        // not copy-able
        // delete move constructors
        InternalHazardPointer(const InternalHazardPointer &other) = delete;

        InternalHazardPointer(InternalHazardPointer &&other) = delete;

        InternalHazardPointer &operator=(const InternalHazardPointer &other) = delete;

        InternalHazardPointer &operator=(InternalHazardPointer &&other) = delete;

        // all fields atomic to ensure thread safety
        std::atomic_flag flag{};
        std::atomic<void *> ptr;
        std::atomic<InternalHazardPointer *> next;
    };

/**
 * HazardPointerList is a list of InternalHazardPointer.
 * It is used to store all hazard pointers.
 * It will free all InternalHazardPointer memory upon destruction.
 */
    class HazardPointerList {
    public:
        HazardPointerList() : head(new InternalHazardPointer()), size(0) {}

        ~HazardPointerList() {
            // HazardPointerList destruct when program exit.
            InternalHazardPointer *p = head.load(std::memory_order_acquire);
            while (p) {
                InternalHazardPointer *temp = p;
                p = p->next.load(std::memory_order_consume);
                delete temp;
            }
        }

        size_t get_size() const { return size.load(std::memory_order_consume); }

        std::atomic<InternalHazardPointer *> head;
        std::atomic<int> size;
    };

}