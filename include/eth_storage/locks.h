//
// Created by Yicheng Zhang on 12/13/23.
//

#include <mutex>  // NOLINT
#include <shared_mutex>

#ifndef LOCK_FREE_EHT_LOCKS_H
#define LOCK_FREE_EHT_LOCKS_H
/**
 * Reader-Writer latch backed by std::mutex.
 */
class ReaderWriterLatch {
public:
    /**
     * Acquire a write latch.
     */
    void WLock() { mutex_.lock(); }

    /**
     * Release a write latch.
     */
    void WUnlock() { mutex_.unlock(); }

    /**
     * Acquire a read latch.
     */
    void RLock() { mutex_.lock_shared(); }

    /**
     * Release a read latch.
     */
    void RUnlock() { mutex_.unlock_shared(); }

private:
    std::shared_mutex mutex_;
};

#endif //LOCK_FREE_EHT_LOCKS_H
