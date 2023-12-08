//
// Created by Chaos Zhai on 12/8/23.
//

#include <shared_mutex>

#define NODE_SIZE 2000

class Node {
public:
    Node() {
        data_ = new char[NODE_SIZE];
        ResetMemory();
    }

    ~Node() {
        delete data_;
    }

    void SetData(char *data) {
        data_ = data;
    }

    char *GetData() {
        return data_;
    }

    inline void WLock() { mutex_.lock(); }
    inline void WUnlock() { mutex_.unlock(); }
    inline void RLock() { mutex_.lock_shared(); }
    inline void RUnlock() { mutex_.unlock_shared(); }

    template<class T>
    const T *As();

    template<class T>
    T *AsMut();

private:
    /** Zeroes out the data. */
    inline void ResetMemory() { memset(data_, 0, NODE_SIZE); }

    char *data_;
    std::shared_mutex mutex_;

};
