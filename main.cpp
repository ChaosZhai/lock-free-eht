#include <iostream>
#include "include/eht.h"

#include "include/coarse-eth.h"
#include "include/fine-eth.h"
#include "lib/comparator/int-comparator.h"
#include "tools/lf_bench.hpp"

#define ASSERT_TRUE(condition) assert(condition)



#define ASSERT_FALSE(condition) assert(!condition)



#define ASSERT_EQ(val1, val2) assert(val1 == val2)



#define ASSERT_NE(val1, val2) GTEST_ASSERT_NE(val1, val2)

using namespace eht;
int main() {

    auto hash_fn = HashFunction<int>();
    auto cmp = IntComparator();
    auto eht = CoarseEHT<int, int, IntComparator>("tools", cmp, hash_fn);
    int num_keys = 8;

    // insert some values
    for (int i = 0; i < num_keys; i++) {
        bool inserted = eht.Insert(i, i);
        assert(inserted);
        auto res = eht.Get(i);
        assert(i == *res);
    }

    num_keys = 10;
    auto eht2 = CoarseEHT<int, int, IntComparator>("tools", cmp, hash_fn);

    // insert some values
    for (int i = 0; i < num_keys; i++) {
        bool inserted = eht2.Insert(i, i);
        ASSERT_TRUE(inserted);
        auto res = eht2.Get(i);

        ASSERT_EQ(i, *res);
    }

    for (int i = 1 << 30; i < num_keys + (1 << 30); i++) {
        bool inserted = eht2.Insert(i, i);
        ASSERT_TRUE(inserted);
        auto res =
                eht2.Get(i);
        ASSERT_EQ(i, *res);
    }



    // check that they were actually inserted
    for (int i = 0; i < num_keys; i++) {

        auto res = eht2.Get(i);

        ASSERT_EQ(i, *res);
    }

    for (int i = 1 << 30; i < num_keys + (1 << 30); i++) {
       auto res = eht2.Get(i);
        ASSERT_TRUE(res.has_value());

        ASSERT_EQ(i, *res);
    }



    // try to get some keys that don't exist/were not inserted
    for (int i = num_keys; i < 2 * num_keys; i++) {
        auto res = eht2.Get(i);
        ASSERT_FALSE(res.has_value());

    }

    auto eht3 = FineEHT<int, int, IntComparator>("test3", cmp, hash_fn);
    num_keys = 8;

    // insert some values
    for (int i = 0; i < num_keys; i++) {
        bool inserted = eht3.Insert(i, i);
        assert(inserted);
        auto res = eht3.Get(i);
        assert(i == *res);
    }

    num_keys = 10;
    auto eht4 = FineEHT<int, int, IntComparator>("test4", cmp, hash_fn);

    // insert some values
    for (int i = 0; i < num_keys; i++) {
        bool inserted = eht4.Insert(i, i);
        ASSERT_TRUE(inserted);
        auto res = eht4.Get(i);

        ASSERT_EQ(i, *res);
    }

    for (int i = 1 << 30; i < num_keys + (1 << 30); i++) {
        bool inserted = eht4.Insert(i, i);
        ASSERT_TRUE(inserted);
        auto res =
                eht4.Get(i);
        ASSERT_EQ(i, *res);
    }



    // check that they were actually inserted
    for (int i = 0; i < num_keys; i++) {

        auto res = eht4.Get(i);

        ASSERT_EQ(i, *res);
    }

    for (int i = 1 << 30; i < num_keys + (1 << 30); i++) {
        auto res = eht4.Get(i);
        ASSERT_TRUE(res.has_value());

        ASSERT_EQ(i, *res);
    }



    // try to get some keys that don't exist/were not inserted
    for (int i = num_keys; i < 2 * num_keys; i++) {
        auto res = eht4.Get(i);
        ASSERT_FALSE(res.has_value());

    }

    std::cout << "All tests passed!" << std::endl;

    lf_bench();
    return 0;
}
