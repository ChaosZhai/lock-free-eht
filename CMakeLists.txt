cmake_minimum_required(VERSION 3.26)
project(lock_free_eht)

set(CMAKE_CXX_STANDARD 17)

add_executable(lock_free_eht main.cpp
        include/eht.h
        src/coarse-eth.cpp
        include/hash_function.h
        lib/murmur3/MurmurHash3.cpp
        include/coarse-eth.h
        lib/comparator/int-comparator.h
        lib/node/node.cpp
        lib/node/node.h
        lib/node/header-node.cpp
        include/eth_storage/htable_header.h
        include/eth_storage/htable_directory.h
        include/common.h
        include/eth_storage/htable_bucket.h
)


