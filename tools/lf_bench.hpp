//
// Created by Chaos Zhai on 12/14/23.
//

#include <iostream>
#include <memory>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../include/lockfree-eht.h"

using namespace eht;

const int kMaxThreads = static_cast<int>(std::thread::hardware_concurrency());

int maxElements;
LockFreeHashTable<int, int> ht;

// Insert sucessfully then ++cnt,  delete succesfully then --cnt.
std::atomic<int> cnt = 0;
std::atomic<bool> start = false;
std::unordered_map<int, int*> elements2timespan;

// Create a random device
std::random_device rd;
// Initialize a random number generator engine
std::default_random_engine engine(rd());
// Define the range of random numbers
std::uniform_int_distribution<int> dist(0, 100);

void onInsert(int divide) {
    while (!start) {
        std::this_thread::yield();
    }

    int n = maxElements / divide;
    for (int i = 0; i < n; ++i) {
        int x = dist(engine) % n;
        if (ht.Insert(x, x)) {
            ++cnt;
        }
    }
}

void onFind(int divide) {
    while (!start) {
        std::this_thread::yield();
    }

    int n = maxElements / divide;
    for (int i = 0; i < n; ++i) {
        int x;
        ht.Get(dist(engine) % n, x);
    }
}

void onDelete(int divide) {
    while (!start) {
        std::this_thread::yield();
    }

    int n = maxElements / divide;
    for (int i = 0; i < n; ++i) {
        if (ht.Remove(dist(engine) % n)) {
            --cnt;
        }
    }
}

void TestConcurrentInsert() {
    int old_size = static_cast<int>(ht.size());
    std::vector<std::thread> threads;
    threads.reserve(kMaxThreads);
    for (int i = 0; i < kMaxThreads; ++i) {
        threads.emplace_back(onInsert, kMaxThreads);
    }

    start = true;
    auto t1_ = std::chrono::steady_clock::now();
    for (int i = 0; i < kMaxThreads; ++i) {
        threads[i].join();
    }
    auto t2_ = std::chrono::steady_clock::now();

    assert(cnt + old_size == static_cast<int>(ht.size()));
    int ms = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(t2_ - t1_).count());
    elements2timespan[maxElements][0] += ms;
    std::cout << maxElements << " elements insert concurrently, timespan=" << ms
              << "ms"
              << "\n";
    start = false;
}

void TestConcurrentFind() {
    std::vector<std::thread> threads;
    threads.reserve(kMaxThreads);
    for (int i = 0; i < kMaxThreads; ++i) {
        threads.emplace_back(onFind, kMaxThreads);
    }

    start = true;
    auto t1_ = std::chrono::steady_clock::now();
    for (int i = 0; i < kMaxThreads; ++i) {
        threads[i].join();
    }
    auto t2_ = std::chrono::steady_clock::now();

    int ms = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(t2_ - t1_).count());
    elements2timespan[maxElements][1] += ms;
    std::cout << maxElements << " elements find concurrently, timespan=" << ms
              << "ms"
              << "\n";

    start = false;
}

void TestConcurrentDelete() {
    int old_size = static_cast<int>(ht.size());
    std::vector<std::thread> threads;
    threads.reserve(kMaxThreads);
    for (int i = 0; i < kMaxThreads; ++i) {
        threads.emplace_back(onDelete, kMaxThreads);
    }

    cnt = 0;
    start = true;
    auto t1_ = std::chrono::steady_clock::now();
    for (int i = 0; i < kMaxThreads; ++i) {
        threads[i].join();
    }
    auto t2_ = std::chrono::steady_clock::now();

    assert(cnt + old_size == static_cast<int>(ht.size()));
    int ms = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(t2_ - t1_).count());
    elements2timespan[maxElements][2] += ms;
    std::cout << maxElements << " elements delete concurrently, timespan=" << ms
              << "ms"
              << "\n";

    cnt = 0;
    start = false;
}

void TestConcurrentInsertAndFindAndDequeue() {
    int old_size = static_cast<int>(ht.size());

    int divide = kMaxThreads / 3;
    std::vector<std::thread> threads;
    for (int i = 0; i < divide; ++i) {
        threads.emplace_back(onInsert, divide);
        threads.emplace_back(onFind, divide);
        threads.emplace_back(onDelete, divide);
    }

    cnt = 0;
    start = true;
    auto t1_ = std::chrono::steady_clock::now();
    for (auto & thread : threads) {
        thread.join();
    }
    auto t2_ = std::chrono::steady_clock::now();

    assert(cnt + old_size == static_cast<int>(ht.size()));
    int ms =static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(t2_ - t1_).count());
    elements2timespan[maxElements][3] += ms;
    std::cout << maxElements
              << " elements insert & find & delete concurrently, timespan=" << ms
              << "ms"
              << "\n";

    cnt = 0;
    start = false;
}

const int kElements1 = 10000;
const int kElements2 = 100000;
const int kElements3 = 1000000;

int lf_bench() {

    srand(std::time(nullptr));

    std::cout << "Benchmark with " << kMaxThreads << " threads:"
              << "\n";

    int elements[] = {kElements1, kElements2, kElements3};
    int timespan1[] = {0, 0, 0, 0};
    int timespan2[] = {0, 0, 0, 0};
    int timespan3[] = {0, 0, 0, 0};

    elements2timespan[kElements1] = timespan1;
    elements2timespan[kElements2] = timespan2;
    elements2timespan[kElements3] = timespan3;

    for (int i = 0; i < 10; ++i) {
        for (int element : elements) {
            maxElements = element;
            TestConcurrentInsert();
            TestConcurrentFind();
            TestConcurrentDelete();
            TestConcurrentInsertAndFindAndDequeue();
            std::cout << "\n";
        }
    }

    for (int element : elements) {
        maxElements = element;
        float avg = static_cast<float>(elements2timespan[maxElements][0]) / 10.0f;
        std::cout << maxElements
                  << " elements insert concurrently, average timespan=" << avg
                  << "ms"
                  << "\n";
        avg = static_cast<float>(elements2timespan[maxElements][1]) / 10.0f;
        std::cout << maxElements
                  << " elements find concurrently, average timespan=" << avg << "ms"
                  << "\n";
        avg = static_cast<float>(elements2timespan[maxElements][2]) / 10.0f;
        std::cout << maxElements
                  << " elements delete concurrently, average timespan=" << avg
                  << "ms"
                  << "\n";
        avg = static_cast<float>(elements2timespan[maxElements][3]) / 10.0f;
        std::cout << maxElements
                  << " elements insert & find & delete concurrently, average timespan="
                  << avg << "ms"
                  << "\n";
        std::cout << "\n";
    }

    return 0;
}
