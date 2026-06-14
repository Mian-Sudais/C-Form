#include "mpsc_queue.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <cassert>

using namespace mpsc;

// Test basic enqueue/dequeue
void test_basic() {
    std::cout << "Test: Basic enqueue/dequeue... ";
    
    MPSCQueue<int> queue;
    
    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);
    
    auto v1 = queue.dequeue();
    auto v2 = queue.dequeue();
    auto v3 = queue.dequeue();
    auto v4 = queue.dequeue();  // Should be empty
    
    assert(v1.has_value() && v1.value() == 1);
    assert(v2.has_value() && v2.value() == 2);
    assert(v3.has_value() && v3.value() == 3);
    assert(!v4.has_value());
    
    std::cout << "PASSED\n";
}

// Test multiple producers
void test_multiple_producers() {
    std::cout << "Test: Multiple producers... ";
    
    MPSCQueue<int> queue;
    std::atomic<int> sum{0};
    constexpr int NUM_PRODUCERS = 4;
    constexpr int ITEMS_PER_PRODUCER = 1000;
    
    std::vector<std::thread> producers;
    
    // Start multiple producer threads
    for (int i = 0; i < NUM_PRODUCERS; ++i) {
        producers.emplace_back([&queue, i]() {
            for (int j = 0; j < ITEMS_PER_PRODUCER; ++j) {
                while (!queue.enqueue(i * ITEMS_PER_PRODUCER + j)) {
                    std::this_thread::yield();
                }
            }
        });
    }
    
    // Consumer thread (single)
    int count = 0;
    int expected_sum = 0;
    for (int i = 0; i < NUM_PRODUCERS; ++i) {
        for (int j = 0; j < ITEMS_PER_PRODUCER; ++j) {
            expected_sum += i * ITEMS_PER_PRODUCER + j;
        }
    }
    
    while (count < NUM_PRODUCERS * ITEMS_PER_PRODUCER) {
        auto val = queue.dequeue();
        if (val.has_value()) {
            sum.fetch_add(val.value(), std::memory_order_relaxed);
            ++count;
        } else {
            std::this_thread::yield();
        }
    }
    
    // Wait for producers to finish
    for (auto& t : producers) {
        t.join();
    }
    
    // Drain any remaining items
    while (auto val = queue.dequeue()) {
        sum.fetch_add(val.value(), std::memory_order_relaxed);
        ++count;
    }
    
    assert(count == NUM_PRODUCERS * ITEMS_PER_PRODUCER);
    assert(sum.load() == expected_sum);
    
    std::cout << "PASSED\n";
}

// Test close functionality
void test_close() {
    std::cout << "Test: Close queue... ";
    
    MPSCQueue<int> queue;
    
    queue.enqueue(1);
    queue.enqueue(2);
    queue.close();
    
    // Can still dequeue existing items
    assert(queue.dequeue().has_value());
    assert(queue.dequeue().has_value());
    
    // Queue should be closed
    assert(queue.is_closed());
    
    // Enqueue should fail after close
    assert(!queue.enqueue(3));
    
    std::cout << "PASSED\n";
}

// Test with move semantics
void test_move_semantics() {
    std::cout << "Test: Move semantics... ";
    
    MPSCQueue<std::string> queue;
    
    std::string s1 = "hello";
    std::string s2 = "world";
    
    queue.enqueue(std::move(s1));
    queue.enqueue(s2);  // Copy
    
    auto v1 = queue.dequeue();
    auto v2 = queue.dequeue();
    
    assert(v1.has_value() && v1.value() == "hello");
    assert(v2.has_value() && v2.value() == "world");
    assert(s1.empty());  // Was moved
    assert(s2 == "world");  // Was copied
    
    std::cout << "PASSED\n";
}

// Stress test with many threads
void test_stress() {
    std::cout << "Test: Stress test (8 producers, 10000 items each)... ";
    
    MPSCQueue<long long> queue;
    std::atomic<long long> sum{0};
    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};
    
    constexpr int NUM_PRODUCERS = 8;
    constexpr int ITEMS_PER_PRODUCER = 10000;
    
    std::vector<std::thread> producers;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Start producers
    for (int i = 0; i < NUM_PRODUCERS; ++i) {
        producers.emplace_back([&queue, &produced, i]() {
            for (int j = 0; j < ITEMS_PER_PRODUCER; ++j) {
                long long val = static_cast<long long>(i) * ITEMS_PER_PRODUCER + j;
                while (!queue.enqueue(val)) {
                    std::this_thread::yield();
                }
                produced.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    
    // Single consumer
    std::thread consumer([&queue, &sum, &consumed]() {
        while (consumed.load(std::memory_order_relaxed) < NUM_PRODUCERS * ITEMS_PER_PRODUCER) {
            auto val = queue.dequeue();
            if (val.has_value()) {
                sum.fetch_add(val.value(), std::memory_order_relaxed);
                consumed.fetch_add(1, std::memory_order_relaxed);
            } else {
                std::this_thread::yield();
            }
        }
    });
    
    // Wait for producers
    for (auto& t : producers) {
        t.join();
    }
    
    // Wait for consumer
    consumer.join();
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Calculate expected sum
    long long expected_sum = 0;
    for (int i = 0; i < NUM_PRODUCERS; ++i) {
        for (int j = 0; j < ITEMS_PER_PRODUCER; ++j) {
            expected_sum += static_cast<long long>(i) * ITEMS_PER_PRODUCER + j;
        }
    }
    
    assert(consumed.load() == NUM_PRODUCERS * ITEMS_PER_PRODUCER);
    assert(sum.load() == expected_sum);
    
    std::cout << "PASSED (" << duration.count() << "ms)\n";
}

// Test epoch-based reclamation doesn't cause use-after-free
void test_memory_safety() {
    std::cout << "Test: Memory safety (EBR)... ";
    
    MPSCQueue<int> queue;
    std::atomic<bool> done{false};
    std::atomic<int> ops{0};
    
    // Rapid enqueue/dequeue cycles to trigger EBR
    std::vector<std::thread> producers;
    
    for (int i = 0; i < 4; ++i) {
        producers.emplace_back([&queue, &done, &ops]() {
            while (!done.load(std::memory_order_acquire)) {
                for (int j = 0; j < 100; ++j) {
                    queue.enqueue(j);
                    ops.fetch_add(1, std::memory_order_relaxed);
                }
                std::this_thread::yield();
            }
        });
    }
    
    std::thread consumer([&queue, &done, &ops]() {
        int local_count = 0;
        while (!done.load(std::memory_order_acquire) || !queue.empty()) {
            auto val = queue.dequeue();
            if (val.has_value()) {
                local_count++;
            } else {
                std::this_thread::yield();
            }
        }
        ops.fetch_add(local_count, std::memory_order_relaxed);
    });
    
    // Run for a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    done.store(true, std::memory_order_release);
    
    // Drain remaining
    while (queue.dequeue().has_value()) {}
    
    for (auto& t : producers) {
        t.join();
    }
    consumer.join();
    
    // If we got here without segfault, EBR is working
    std::cout << "PASSED\n";
}

int main() {
    std::cout << "=== MPSC Lock-Free Queue Tests ===\n\n";
    
    test_basic();
    test_multiple_producers();
    test_close();
    test_move_semantics();
    test_stress();
    test_memory_safety();
    
    std::cout << "\n=== All tests PASSED! ===\n";
    
    return 0;
}
