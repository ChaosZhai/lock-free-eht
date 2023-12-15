//
// Created by Chaos Zhai on 12/13/23.
//
#pragma once

#include "reclaimer.h"

namespace eht {

    class HazardPointer {
    public:
        HazardPointer() : reclaimer_(nullptr), index(Reclaimer::HP_INDEX_NULL) {}

        HazardPointer(Reclaimer *reclaimer_, void *ptr)
                : reclaimer_(reclaimer_), index(reclaimer_->MarkHazard(ptr)) {}

        ~HazardPointer() { UnMark(); }

        HazardPointer(const HazardPointer &other) = delete;

        HazardPointer(HazardPointer &&other) noexcept {
            *this = std::move(other);
        }

        HazardPointer &operator=(const HazardPointer &other) = delete;

        HazardPointer &operator=(HazardPointer &&other) noexcept {
            reclaimer_ = other.reclaimer_;
            index = other.index;

            // If move assign is called, we must disable the other's index to avoid
            // incorrectly unmark index when other destruct.
            other.index = Reclaimer::HP_INDEX_NULL;
            return *this;
        }

        void UnMark() const { reclaimer_->UnMarkHazard(index); }

    public:
        Reclaimer *reclaimer_{};
        int index{};
    };

} // namespace eht