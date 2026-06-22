#pragma once

#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <algorithm>
#include <cstdint>
#include <functional>


#include "message_queue_interface.hpp"


namespace message_queue {

template<
  MessageType ValueType,
  ThreadAccessCategory ThreadCategory,
  DeadlockExceptionPolicy ExceptionPolicy,
  typename Compare = std::less<ValueType>
>
class PriorityMessageQueue : public IMessageQueue<ValueType, ThreadCategory, ExceptionPolicy> {
 private:
  struct HeapNode {
    uint64_t sequence_id;
    ValueType data;
  };

  struct MinHeapComparator {
    Compare comp;

    bool operator()(const HeapNode& lhs, const HeapNode& rhs) const {
      if (comp(rhs.data, lhs.data)) {
        return true;
      }
      if (comp(lhs.data, rhs.data)) {
        return false;
      }
      return lhs.sequence_id > rhs.sequence_id;
    }
  };

  std::size_t capacity_;
  std::atomic<std::size_t> size_{0};
  std::atomic<bool> closed_{false};
  uint64_t current_seq_ = 0;

  std::vector<HeapNode> heap_;
  mutable std::mutex mutex_;
  std::condition_variable not_full_;
  std::condition_variable not_empty_;
  Compare comp_instance_;

 protected:
  bool CheckSendDeadlockPossibility() const noexcept override {
    if constexpr (ThreadCategory == ThreadAccessCategory::kSingleProducerSingleConsumer ||
                  ThreadCategory == ThreadAccessCategory::kMultipleProducerSingleConsumer) {
      if (this->IsBothRole() && size_.load(std::memory_order_acquire) >= capacity_) {
        return true;
      }
    }
    return false;
  }

  void SyncAndOverflowPrework() override {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_.wait(lock, [this]() {
      return size_.load(std::memory_order_acquire) < capacity_ || closed_.load(std::memory_order_acquire);
    });

    if (closed_.load(std::memory_order_acquire)) {
      throw MessageQueueException("Queue is closed");
    }

    lock.release();
  }

  bool TrySyncAndOverflowPrework() noexcept override {
    mutex_.lock();
    if (size_.load(std::memory_order_acquire) >= capacity_ || closed_.load(std::memory_order_acquire)) {
      mutex_.unlock();
      return false;
    }
    return true;
  }

  void StoreMessage(const ValueType& message) override {
    try {
      heap_.push_back({current_seq_++, message});
      std::push_heap(heap_.begin(), heap_.end(), MinHeapComparator{comp_instance_});
      size_.store(heap_.size(), std::memory_order_release);
    } catch (...) {
      mutex_.unlock();
      throw;
    }
  }

  void StoreMessage(ValueType&& message) override {
    try {
      heap_.push_back({current_seq_++, std::move(message)});
      std::push_heap(heap_.begin(), heap_.end(), MinHeapComparator{comp_instance_});
      size_.store(heap_.size(), std::memory_order_release);
    } catch (...) {
      mutex_.unlock();
      throw;
    }
  }

  void SendPostwork() override {
    mutex_.unlock();
    not_empty_.notify_one();
  }

  bool CheckReadDeadlockPossibility() const noexcept override {
    if constexpr (ThreadCategory == ThreadAccessCategory::kSingleProducerSingleConsumer ||
                  ThreadCategory == ThreadAccessCategory::kSingleProducerMultipleConsumer) {
      if (this->IsBothRole() && size_.load(std::memory_order_acquire) == 0) {
        return true;
      }
    }
    return false;
  }

  void SyncAndUnderflowPrework() override {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this]() {
      return size_.load(std::memory_order_acquire) > 0 || closed_.load(std::memory_order_acquire);
    });

    if (size_.load(std::memory_order_acquire) == 0 && closed_.load(std::memory_order_acquire)) {
      throw MessageQueueException("Queue is closed and empty");
    }

    lock.release();
  }

  bool TrySyncAndUnderflowPrework() noexcept override {
    mutex_.lock();
    if (size_.load(std::memory_order_acquire) == 0) {
      mutex_.unlock();
      return false;
    }
    return true;
  }

  ValueType PopMessage() override {
    try {
      std::pop_heap(heap_.begin(), heap_.end(), MinHeapComparator{comp_instance_});
      ValueType message = std::move(heap_.back().data);
      heap_.pop_back();
      size_.store(heap_.size(), std::memory_order_release);
      return message;
    } catch (...) {
      mutex_.unlock();
      throw;
    }
  }

  void ReadPostwork() override {
    mutex_.unlock();
    not_full_.notify_one();
  }

 public:
  explicit PriorityMessageQueue(std::size_t capacity, Compare comp = Compare())
      : capacity_(capacity), comp_instance_(std::move(comp)) {
    if (capacity == 0) {
      throw MessageQueueException("Capacity must be greater than zero");
    }
    heap_.reserve(capacity_);
  }

  std::size_t Size() const noexcept override {
    return size_.load(std::memory_order_acquire);
  }

  bool Empty() const noexcept override {
    return size_.load(std::memory_order_acquire) == 0;
  }

  void Close() override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      closed_.store(true, std::memory_order_release);
    }
    not_full_.notify_all();
    not_empty_.notify_all();
  }
};

}