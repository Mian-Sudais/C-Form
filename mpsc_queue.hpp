#ifndef MPSC_QUEUE_HPP
#define MPSC_QUEUE_HPP

#include <atomic>
#include <optional>
#include <cstdint>
#include <array>
#include <thread>
#include <vector>

/**
 * Lock-free MPSC Queue with Epoch-Based Reclamation (EBR)
 * 
 * Features:
 * - Multiple Producers, Single Consumer
 * - No mutexes, semaphores, or condition variables
 * - Epoch-based reclamation for safe memory management
 * - Explicit memory ordering (no seq_cst)
 * - No shared_ptr overhead
 */

namespace mpsc {

// Forward declaration
template<typename T>
class Queue;

// Base class for nodes to enable type-erased destruction (must be declared early)
struct NodeBase {
    std::atomic<void*> next_retired{nullptr};
    virtual void destroy() = 0;
    virtual ~NodeBase() = default;
};

/**
 * Epoch-Based Reclamation Manager
 * 
 * EBR works by maintaining global epochs and tracking which threads
 * are active in which epochs. Nodes are only reclaimed when no thread
 * can possibly hold a reference to them (i.e., all threads have moved
 * past the epoch in which the node was retired).
 */
class EpochManager {
public:
    static constexpr size_t MAX_THREADS = 256;
    static constexpr size_t NUM_EPOCHS = 3;  // Need at least 3 epochs
    static constexpr size_t RETIRE_THRESHOLD = 128;

private:
    struct ThreadState {
        std::atomic<uint64_t> local_epoch{0};
        std::atomic<bool> active{false};
        char padding[64 - sizeof(std::atomic<uint64_t>) - sizeof(std::atomic<bool>)];
    };

    std::atomic<uint64_t> global_epoch{0};
    std::array<ThreadState, MAX_THREADS> thread_states;
    
    // Per-thread retire lists for each epoch
    struct RetireList {
        std::atomic<void*> head{nullptr};
        std::atomic<size_t> count{0};
    };
    
    std::array<std::array<RetireList, NUM_EPOCHS>, MAX_THREADS> retire_lists;
    
    std::atomic<size_t> thread_counter{0};

public:
    EpochManager() = default;
    
    ~EpochManager() {
        // Force reclaim all remaining nodes from all epochs for all threads
        for (size_t i = 0; i < MAX_THREADS; ++i) {
            for (size_t e = 0; e < NUM_EPOCHS; ++e) {
                RetireList& rl = retire_lists[i][e];
                void* node = rl.head.exchange(nullptr, std::memory_order_acquire);
                
                while (node) {
                    auto* n = static_cast<NodeBase*>(node);
                    void* next = n->next_retired.load(std::memory_order_relaxed);
                    n->destroy();
                    node = next;
                }
            }
        }
    }
    
    // Register a thread and get its ID
    size_t register_thread() {
        return thread_counter.fetch_add(1, std::memory_order_relaxed);
    }
    
    void unregister_thread(size_t thread_id) {
        if (thread_id < MAX_THREADS) {
            // Try to reclaim any remaining nodes before exiting
            try_reclaim(thread_id);
        }
    }
    
    // Enter a critical section (protecting epoch)
    void enter_critical(size_t thread_id) {
        if (thread_id >= MAX_THREADS) return;
        
        uint64_t ge = global_epoch.load(std::memory_order_relaxed);
        thread_states[thread_id].local_epoch.store(ge, std::memory_order_relaxed);
        thread_states[thread_id].active.store(true, std::memory_order_release);
        
        // Re-read global epoch after setting active flag (acquire fence)
        std::atomic_thread_fence(std::memory_order_acquire);
        ge = global_epoch.load(std::memory_order_acquire);
        thread_states[thread_id].local_epoch.store(ge, std::memory_order_relaxed);
    }
    
    // Exit a critical section
    void exit_critical(size_t thread_id) {
        if (thread_id >= MAX_THREADS) return;
        thread_states[thread_id].active.store(false, std::memory_order_release);
    }
    
    // RAII guard for critical section
    class Guard {
        EpochManager& mgr;
        size_t tid;
    public:
        Guard(EpochManager& m, size_t t) : mgr(m), tid(t) {
            mgr.enter_critical(tid);
        }
        ~Guard() {
            mgr.exit_critical(tid);
        }
        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;
    };
    
    Guard guard(size_t thread_id) {
        return Guard(*this, thread_id);
    }
    
    // Retire a node for later reclamation
    template<typename Node>
    void retire(size_t thread_id, Node* node) {
        if (thread_id >= MAX_THREADS || !node) return;
        
        uint64_t epoch = global_epoch.load(std::memory_order_acquire) % NUM_EPOCHS;
        
        // Push onto the retire list for this epoch (lock-free stack)
        RetireList& rl = retire_lists[thread_id][epoch];
        void* old_head = rl.head.load(std::memory_order_relaxed);
        
        // We store the next pointer in the node itself
        // The node must have a 'next_retired' member or we use a wrapper
        node->next_retired.store(old_head, std::memory_order_relaxed);
        
        while (!rl.head.compare_exchange_weak(old_head, node,
                std::memory_order_release, std::memory_order_relaxed)) {
            node->next_retired.store(old_head, std::memory_order_relaxed);
        }
        
        size_t count = rl.count.fetch_add(1, std::memory_order_relaxed) + 1;
        
        // Try to reclaim if we've accumulated enough nodes
        if (count >= RETIRE_THRESHOLD) {
            try_reclaim(thread_id);
        }
    }
    
    // Try to advance the epoch and reclaim old nodes
    void try_reclaim(size_t /* thread_id */) {
        uint64_t current_epoch = global_epoch.load(std::memory_order_acquire);
        
        // Check if we can advance the epoch
        bool can_advance = true;
        for (size_t i = 0; i < MAX_THREADS; ++i) {
            if (thread_states[i].active.load(std::memory_order_acquire)) {
                uint64_t le = thread_states[i].local_epoch.load(std::memory_order_acquire);
                if (le < current_epoch) {
                    can_advance = false;
                    break;
                }
            }
        }
        
        if (can_advance) {
            // Try to advance the global epoch
            uint64_t new_epoch = current_epoch + 1;
            if (global_epoch.compare_exchange_strong(current_epoch, new_epoch,
                    std::memory_order_release, std::memory_order_relaxed)) {
                // Successfully advanced, now reclaim nodes from epoch-2 (modulo NUM_EPOCHS)
                uint64_t reclaim_epoch = (new_epoch + 1) % NUM_EPOCHS;
                
                // Reclaim from all thread retire lists for that epoch
                for (size_t i = 0; i < MAX_THREADS; ++i) {
                    RetireList& rl = retire_lists[i][reclaim_epoch];
                    
                    // Pop all nodes from the retire list
                    void* node = rl.head.exchange(nullptr, std::memory_order_acquire);
                    rl.count.store(0, std::memory_order_relaxed);
                    
                    while (node) {
                        // Cast back to Node* and delete
                        // We need to know the actual type, so we use a template approach
                        // For now, we'll store a deleter function pointer or use CRTP
                        // This is simplified - in practice you'd have type-erased deletion
                        auto* n = static_cast<NodeBase*>(node);
                        void* next = n->next_retired.load(std::memory_order_relaxed);
                        n->destroy();  // Type-erased destruction
                        node = next;
                    }
                }
            }
        }
    }
};

/**
 * Lock-free MPSC Queue Node
 */
template<typename T>
struct Node : public NodeBase {
    T data;
    std::atomic<Node<T>*> next{nullptr};
    
    Node() = default;
    explicit Node(const T& val) : data(val) {}
    explicit Node(T&& val) : data(std::move(val)) {}
    
    void destroy() override {
        delete this;
    }
};

/**
 * Lock-free MPSC Queue with Epoch-Based Reclamation
 * 
 * Algorithm: Based on the classic Michael & Scott queue adapted for MPSC
 * - Producers can only enqueue at the tail
 * - Only the single consumer can dequeue from the head
 * - Uses a sentinel/dummy node to simplify concurrency
 */
template<typename T>
class Queue {
private:
    using NodeType = Node<T>;
    
    // Cache-line aligned pointers to avoid false sharing
    alignas(64) std::atomic<NodeType*> head{nullptr};
    alignas(64) std::atomic<NodeType*> tail{nullptr};
    alignas(64) std::atomic<bool> closed{false};
    
    EpochManager& epoch_mgr;
    size_t thread_id;

public:
    explicit Queue(EpochManager& mgr, size_t tid) 
        : epoch_mgr(mgr), thread_id(tid) 
    {
        // Initialize with a sentinel node
        NodeType* sentinel = new NodeType();
        head.store(sentinel, std::memory_order_relaxed);
        tail.store(sentinel, std::memory_order_relaxed);
    }
    
    ~Queue() {
        // Drain the queue
        while (dequeue().has_value()) {}
        
        // Delete the sentinel node
        NodeType* sentinel = head.load(std::memory_order_relaxed);
        delete sentinel;
    }
    
    // Non-copyable, non-movable
    Queue(const Queue&) = delete;
    Queue& operator=(const Queue&) = delete;
    Queue(Queue&&) = delete;
    Queue& operator=(Queue&&) = delete;
    
    /**
     * Enqueue an element (Multiple Producers)
     * Returns true on success, false if queue is closed
     */
    bool enqueue(const T& value) {
        return enqueue_impl([&]() { return new NodeType(value); });
    }
    
    bool enqueue(T&& value) {
        return enqueue_impl([&]() { return new NodeType(std::move(value)); });
    }
    
    /**
     * Dequeue an element (Single Consumer only)
     * Returns std::nullopt if queue is empty, or the value if successful
     * Returns std::nullopt if queue is closed and empty
     */
    std::optional<T> dequeue() {
        auto guard = epoch_mgr.guard(thread_id);
        
        while (true) {
            NodeType* h = head.load(std::memory_order_acquire);
            NodeType* t = tail.load(std::memory_order_acquire);
            NodeType* next = h->next.load(std::memory_order_acquire);
            
            // Check consistency
            if (h == head.load(std::memory_order_acquire)) {
                if (h == t) {
                    // Queue appears empty
                    if (!next) {
                        // Truly empty
                        return std::nullopt;
                    }
                    // Tail is lagging, try to advance it
                    tail.compare_exchange_weak(t, next,
                        std::memory_order_release, std::memory_order_relaxed);
                } else {
                    // Queue has elements
                    if (!next) {
                        // Inconsistent state, retry
                        continue;
                    }
                    
                    // Read value before CAS (other producers might access next)
                    T result = std::move(next->data);
                    
                    // Try to swing head to next
                    if (head.compare_exchange_weak(h, next,
                            std::memory_order_release, std::memory_order_relaxed)) {
                        // Success! Retire the old head (sentinel)
                        epoch_mgr.retire(thread_id, h);
                        return result;
                    }
                    // CAS failed, retry
                }
            }
        }
    }
    
    /**
     * Close the queue - no more enqueues allowed
     */
    void close() {
        closed.store(true, std::memory_order_release);
    }
    
    /**
     * Check if queue is closed
     */
    bool is_closed() const {
        return closed.load(std::memory_order_acquire);
    }
    
    /**
     * Check if queue is empty (approximate, may race)
     */
    bool empty() const {
        NodeType* h = head.load(std::memory_order_acquire);
        NodeType* t = tail.load(std::memory_order_acquire);
        return h == t || !h->next.load(std::memory_order_acquire);
    }

private:
    template<typename Factory>
    bool enqueue_impl(Factory&& factory) {
        // Check if closed first (optimization)
        if (closed.load(std::memory_order_acquire)) {
            return false;
        }
        
        auto guard = epoch_mgr.guard(thread_id);
        
        NodeType* node = factory();
        
        while (true) {
            NodeType* t = tail.load(std::memory_order_acquire);
            NodeType* next = t->next.load(std::memory_order_acquire);
            
            // Check tail consistency
            if (t == tail.load(std::memory_order_acquire)) {
                if (!next) {
                    // Tail is pointing to last node, try to link new node
                    if (t->next.compare_exchange_weak(next, node,
                            std::memory_order_release, std::memory_order_relaxed)) {
                        // Successfully linked, try to advance tail
                        tail.compare_exchange_weak(t, node,
                            std::memory_order_release, std::memory_order_relaxed);
                        return true;
                    }
                    // CAS failed, retry
                } else {
                    // Tail is lagging, help advance it
                    tail.compare_exchange_weak(t, next,
                        std::memory_order_release, std::memory_order_relaxed);
                }
            }
            
            // Check again if closed after acquiring guard
            if (closed.load(std::memory_order_acquire)) {
                // Clean up the node we allocated
                delete node;
                return false;
            }
        }
    }
};

/**
 * Thread-local storage for thread IDs
 */
class ThreadIdManager {
private:
    static thread_local size_t cached_id;
    static std::atomic<size_t> next_id;
    static EpochManager global_epoch_mgr;
    
public:
    static size_t get_id() {
        if (cached_id == static_cast<size_t>(-1)) {
            cached_id = next_id.fetch_add(1, std::memory_order_relaxed);
        }
        return cached_id;
    }
    
    static EpochManager& get_epoch_manager() {
        return global_epoch_mgr;
    }
};

// Static member definitions
thread_local size_t ThreadIdManager::cached_id = static_cast<size_t>(-1);
std::atomic<size_t> ThreadIdManager::next_id{0};
EpochManager ThreadIdManager::global_epoch_mgr{};

/**
 * Convenience class: Self-contained MPSC Queue
 * Creates its own EpochManager internally
 */
template<typename T>
class MPSCQueue {
private:
    EpochManager epoch_mgr;
    Queue<T>* queue{nullptr};
    size_t consumer_tid;
    bool initialized{false};

public:
    MPSCQueue() {
        consumer_tid = epoch_mgr.register_thread();
        queue = new Queue<T>(epoch_mgr, consumer_tid);
        initialized = true;
    }
    
    ~MPSCQueue() {
        if (initialized) {
            delete queue;
            epoch_mgr.unregister_thread(consumer_tid);
        }
    }
    
    // Non-copyable, non-movable
    MPSCQueue(const MPSCQueue&) = delete;
    MPSCQueue& operator=(const MPSCQueue&) = delete;
    MPSCQueue(MPSCQueue&&) = delete;
    MPSCQueue& operator=(MPSCQueue&&) = delete;
    
    /**
     * Producer: Enqueue an element
     * Returns true on success, false if queue is closed
     */
    bool enqueue(const T& value) {
        size_t tid = epoch_mgr.register_thread();
        auto guard = epoch_mgr.guard(tid);
        
        // Create a temporary queue view for this producer
        // We need to properly initialize head/tail pointers
        bool result = queue->enqueue(value);
        
        // Note: In production, you'd want to cache thread IDs per producer
        // rather than registering/unregistering each time
        epoch_mgr.unregister_thread(tid);
        
        return result;
    }
    
    bool enqueue(T&& value) {
        size_t tid = epoch_mgr.register_thread();
        auto guard = epoch_mgr.guard(tid);
        
        bool result = queue->enqueue(std::move(value));
        
        epoch_mgr.unregister_thread(tid);
        
        return result;
    }
    
    /**
     * Consumer: Dequeue an element (MUST be called from single consumer thread)
     */
    std::optional<T> dequeue() {
        return queue->dequeue();
    }
    
    /**
     * Close the queue
     */
    void close() {
        queue->close();
    }
    
    bool is_closed() const {
        return queue->is_closed();
    }
    
    bool empty() const {
        return queue->empty();
    }
};

} // namespace mpsc

#endif // MPSC_QUEUE_HPP
