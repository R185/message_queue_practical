#pragma once
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <optional>
#include <vector>
#include <mutex>
#include <utility>

#include "exception.hpp"
#include "message_queue_interface.hpp"

namespace message_queue {

template<typename T>
struct Node {
    T value;
    std::atomic<Node<T>*> next;

    Node()
        : next(nullptr)
    {}

    Node(T val)
        : value(std::move(val))
        , next(nullptr)
    {}
};

struct QueueStats {
    std::atomic<size_t> cas_retries{0};
    std::atomic<size_t> successful_operations{0};
};

struct QueueStatsSnapshot {
    size_t cas_retries = 0;
    size_t successful_operations = 0;
};

template<
  message_queue::MessageType T,
  message_queue::ThreadAccessCategory Category,
  message_queue::DeadlockExceptionPolicy Policy,
  typename Allocator = std::allocator<Node<T>>
>
class MichaelScottQueue : public message_queue::IMessageQueue<T, Category, Policy> {
    std::atomic<Node<T>*> head_;
    std::atomic<Node<T>*> tail_;

    std::vector<Node<T>*> retired_nodes_;
    std::mutex retired_mtx_;

    std::mutex mtx_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;

    std::atomic<size_t> current_size_{0};
    const size_t kCapacity_ = 0;
    std::atomic<bool> is_closed{false};

    QueueStats stats_;
    Allocator allocator_;

    bool TryStoreInternal(T&& value) {
        Node<T>* new_node = allocator_.allocate(1);
        new (new_node) Node<T>{std::move(value)};

        constexpr int kMaxAttempts = 64;
        for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
            Node<T>* last = tail_.load(std::memory_order_acquire);
            Node<T>* next = last->next.load(std::memory_order_acquire);

            if (last != tail_.load(std::memory_order_acquire)) {
                continue;
            }

            if (next == nullptr) {
                if (last->next.compare_exchange_weak(
                        next, new_node, std::memory_order_release, std::memory_order_relaxed)) {
                    tail_.compare_exchange_strong(
                        last, new_node, std::memory_order_release, std::memory_order_relaxed);
                    return true;
                }
                stats_.cas_retries.fetch_add(1, std::memory_order_relaxed);
            } else {
                tail_.compare_exchange_strong(
                    last, next, std::memory_order_release, std::memory_order_relaxed);
                stats_.cas_retries.fetch_add(1, std::memory_order_relaxed);
            }
        }

        new_node->~Node();
        allocator_.deallocate(new_node, 1);
        return false;
    }

    void StoreInternal(T&& value) {
        Node<T>* new_node = allocator_.allocate(1);
        new (new_node) Node<T>{std::move(value)};

        while (true) {
            Node<T>* last = tail_.load(std::memory_order_acquire);
            Node<T>* next = last->next.load(std::memory_order_acquire);

            if (last == tail_.load(std::memory_order_acquire)) {
                if (next == nullptr) {
                    if (last->next.compare_exchange_weak(
                            next, new_node, std::memory_order_release, std::memory_order_relaxed)) {
                        tail_.compare_exchange_strong(
                            last, new_node, std::memory_order_release, std::memory_order_relaxed);

                        return;
                    }

                    stats_.cas_retries.fetch_add(1, std::memory_order_relaxed);
                } else {
                    tail_.compare_exchange_strong(
                        last, next, std::memory_order_release, std::memory_order_relaxed);

                    stats_.cas_retries.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    }

    bool TryPopOnce(T& out) {
        Node<T>* first = head_.load(std::memory_order_acquire);
        Node<T>* last = tail_.load(std::memory_order_acquire);
        Node<T>* next = first->next.load(std::memory_order_acquire);

        if (first != head_.load(std::memory_order_acquire)) {
            stats_.cas_retries.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        if (first == last) {
            if (next == nullptr) {
                return false;
            }

            tail_.compare_exchange_strong(last, next, std::memory_order_release, std::memory_order_relaxed);

            return false;
        }

        if (next == nullptr) {
            return false;
        }

        if (head_.compare_exchange_weak(first, next, std::memory_order_release, std::memory_order_relaxed)) {
            out = std::move(next->value);

            {
                std::lock_guard<std::mutex> lock(retired_mtx_);
                retired_nodes_.push_back(first);
            }

            return true;
        }

        stats_.cas_retries.fetch_add(1, std::memory_order_relaxed);

        return false;
    }

    protected:
        void SyncAndOverflowPrework() override {
            std::unique_lock<std::mutex> lock(mtx_);

            not_full_.wait(lock, [this] {
                return current_size_.load() < kCapacity_ || is_closed.load();
            });

            if (is_closed.load()) {
                throw message_queue::MessageQueueException("Closed");
            }
        }

        bool TrySyncAndOverflowPrework() noexcept override {
            return !is_closed.load() && current_size_.load() < kCapacity_;
        }

        void StoreMessage(T&& value) override {
            StoreInternal(std::move(value)); 
        }

        void StoreMessage(const T& value) override {
            T copy = value;

            StoreInternal(std::move(copy));
        }

        void SendPostwork() override {
            current_size_.fetch_add(1);

            stats_.successful_operations.fetch_add(1);

            not_empty_.notify_one();
        }

        void SyncAndUnderflowPrework() override {
            std::unique_lock<std::mutex> lock(mtx_);
            
            not_empty_.wait(lock, [this] {
                return current_size_.load() > 0 || is_closed.load();
            });

            if (is_closed.load() && current_size_.load() == 0) {
                throw message_queue::MessageQueueException("Empty and Closed");
            }
        }

        bool TrySyncAndUnderflowPrework() noexcept override {
            return current_size_.load() > 0;
        }

        T PopMessage() override {
            while (true) {
                T result{};

                if (TryPopOnce(result)) {
                    return result;
                }
            }
        }

        void ReadPostwork() override {
            current_size_.fetch_sub(1);

            stats_.successful_operations.fetch_add(1);

            not_full_.notify_one();
        }

        bool CheckSendDeadlockPossibility() const noexcept override {
            return this->IsBothRole() && current_size_.load() >= kCapacity_;
        }

        bool CheckReadDeadlockPossibility() const noexcept override {
            return this->IsBothRole() && current_size_.load() == 0;
        }

    public:
        MichaelScottQueue(const MichaelScottQueue&) = delete;
        MichaelScottQueue& operator=(const MichaelScottQueue&) = delete;
        MichaelScottQueue(MichaelScottQueue&&) = delete;
        MichaelScottQueue& operator=(MichaelScottQueue&&) = delete;

        MichaelScottQueue(size_t max_capacity)
            : kCapacity_(max_capacity)
        {
            if (max_capacity == 0) {
                throw MessageQueueException("Capacity must be greater than zero");
            }

            Node<T>* dummy = allocator_.allocate(1);
            new (dummy) Node<T>{};

            head_.store(dummy);
            tail_.store(dummy);
        }

        ~MichaelScottQueue() {
            while (Node<T>* current = head_.load()) {
                head_.store(current->next.load());

                current->~Node();

                allocator_.deallocate(current, 1);
            }

            for (Node<T>* current : retired_nodes_) {
                current->~Node();
                
                allocator_.deallocate(current, 1);
            }
        }

        size_t Size() const noexcept override {
            return current_size_.load();
        }

        bool Empty() const noexcept override {
            return current_size_.load() == 0;
        }

        void Close() override {
            is_closed.store(true);

            not_empty_.notify_all();

            not_full_.notify_all();
        }

        bool TryEnqueue(T&& value) noexcept {
            if (!TrySyncAndOverflowPrework()) {
                return false;
            }

            if (!TryStoreInternal(std::move(value))) {
                return false;
            }

            SendPostwork();

            return true;
        }

        bool TryEnqueue(const T& value) noexcept {
            T copy = value;

            return TryEnqueue(std::move(copy));
        }

        std::optional<T> TryDequeue() noexcept {
            if (!TrySyncAndUnderflowPrework()) {
                return std::nullopt;
            }

            T value{};

            if (!TryPopOnce(value)) {
                return std::nullopt;
            }

            ReadPostwork();
            
            return value;
        }
        
        QueueStatsSnapshot Stats() const {
            return {stats_.cas_retries.load(), stats_.successful_operations.load()};
        };
};

}  // namespace message_queue
