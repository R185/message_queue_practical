#pragma once
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <vector>
#include <mutex>
#include <utility>
#include <optional>

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

template<typename T, typename Allocator = std::allocator<Node<T>>>
class Queue {
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

    void StoreMessage(T value) {
        Node<T>* new_node = allocator_.allocate(1);
        new (new_node) Node<T>{std::move(value)};

        while (true) {
            Node<T>* last = tail_.load();
            Node<T>* next = last->next.load();

            if (last == tail_.load()) {
                if (next == nullptr) {
                    if (last->next.compare_exchange_weak(next, new_node)) {
                        tail_.compare_exchange_strong(last, new_node);

                        return;
                    }

                    stats_.cas_retries.fetch_add(1);
                } else {
                    tail_.compare_exchange_strong(last, next);

                    stats_.cas_retries.fetch_add(1);
                }
            }
        }
    }

    std::optional<T> PopMessage() {
        while(true) {
            Node<T>* first = head_.load();
            Node<T>* last = tail_.load();
            Node<T>* next = first->next.load();

            if (first == head_.load()) {
                if (first == last) {
                    if (next == nullptr) {
                        return std::nullopt;
                    }
                    
                    tail_.compare_exchange_strong(last, next);
                } else {
                    if (next == nullptr) {
                        continue;
                    }

                    if (head_.compare_exchange_weak(first, next)) {
                        T res = std::move(next->value);

                        {
                            std::lock_guard<std::mutex> lock(retired_mtx_);

                            retired_nodes_.push_back(first);
                        }
                        
                        return res;
                    }
                }
            }
            
            stats_.cas_retries.fetch_add(1);
        }
    }

    public:
        Queue(size_t max_capacity)
            : kCapacity_(max_capacity)
        {
            Node<T>* dummy = allocator_.allocate(1);
            new (dummy) Node<T>{};

            head_.store(dummy);
            tail_.store(dummy);
        }

        ~Queue() {
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

        bool Send(T value) {
            {
                std::unique_lock<std::mutex> lock(mtx_);

                not_full_.wait(lock, [this] {
                    return current_size_.load() < kCapacity_ || is_closed.load();
                });

                if (is_closed.load()) {
                    return false;
                }
            }

            StoreMessage(std::move(value));

            current_size_.fetch_add(1);
            stats_.successful_operations.fetch_add(1);
            not_empty_.notify_one();

            return true;
        };
        
        bool TrySend(T value) {
            if (is_closed.load() || current_size_.load() >= kCapacity_) {
                return false;
            }

            StoreMessage(std::move(value));

            current_size_.fetch_add(1);
            stats_.successful_operations.fetch_add(1);
            not_empty_.notify_one();

            return true;
        };
        
        std::optional<T> Read() {
            while (true) {
                {
                    std::unique_lock<std::mutex> lock(mtx_);
                    not_empty_.wait(lock, [this] {
                        return current_size_.load() > 0 || is_closed.load();
                    });

                    if (is_closed.load() && current_size_.load() == 0) {
                        return std::nullopt;
                    }
                }

                std::optional<T> result = PopMessage();

                if (result) {
                    current_size_.fetch_sub(1);
                    stats_.successful_operations.fetch_add(1);
                    not_full_.notify_one();
                    
                    return result;
                }

            }
        }
        
        std::optional<T> TryRead() {
            if (current_size_.load() == 0) {
                return std::nullopt;
            }

            auto result = PopMessage();

            if (result) {
                current_size_.fetch_sub(1);
                stats_.successful_operations.fetch_add(1);
                
                not_full_.notify_one();
            }
            
            return result;
        }
        
        size_t Size() const {
            return current_size_.load();
        };
        
        QueueStatsSnapshot Stats() const {
            return {stats_.cas_retries.load(), stats_.successful_operations.load()};
        };
        
        void Close() {
            is_closed.store(true);

            not_empty_.notify_all();

            not_full_.notify_all();
        };
};
