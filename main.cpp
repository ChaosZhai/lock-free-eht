#include <iostream>
#include "include/coarse-eth.h"
#include "lib/comparator/int-comparator.h"

int main() {
    std::cout << "Hello, World!" << std::endl;
    auto hash_fn = HashFunction<int>();
    auto cmp = IntComparator();
    auto eht = CoarseEHT<int, int, IntComparator>("test", cmp, hash_fn);


    return 0;
}
