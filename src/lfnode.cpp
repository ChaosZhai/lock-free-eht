//
// Created by Chaos Zhai on 12/14/23.
//
#include "../include/lockfree_helpers/lfnode.h"
namespace eht {

    bool is_marked_reference(LFNode *next) {
    return (reinterpret_cast<unsigned long>(next)& 0x1) == 0x1;
}

    LFNode *get_marked_reference(LFNode *next) {
        return reinterpret_cast<LFNode *>(reinterpret_cast<unsigned long>(next) | 0x1);
    }

    LFNode *get_unmarked_reference(LFNode *next) {
        return reinterpret_cast<LFNode *>(reinterpret_cast<unsigned long>(next) &
                                          ~0x1);
    }

} // namespace eht