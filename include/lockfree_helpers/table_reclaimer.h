//
// Created by Chaos Zhai on 12/14/23.
//
#pragma once
#include "../../lib/hazardPointer/reclaimer.h"

namespace eht {

    template<typename K, typename V>
    class TableReclaimer : public Reclaimer {

    public:
        explicit TableReclaimer(HazardPointerList &hp_list) : Reclaimer(hp_list) {}

        ~TableReclaimer() = default;

        static TableReclaimer<K, V> &GetInstance(HazardPointerList &hp_list) {
            thread_local static TableReclaimer reclaimer(
                    hp_list);  // thread_local: each thread has its own instance.
            return reclaimer;
        }
    };

}