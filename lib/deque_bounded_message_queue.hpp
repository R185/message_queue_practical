#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <utility>

#include "message_queue_interface.hpp"

namespace message_queue {

template <MessageType ValueType, ThreadAccessCategory ThreadCategory,
          DeadlockExceptionPolicy ExceptionPolicy,
          typename Container = std::deque<ValueType>>
class DequeBoundedMessageQueue final
    : public IMessageQueue<ValueType, ThreadCategory, ExceptionPolicy> {
 public:
  explicit DequeBoundedMessageQueue(std::size_t capacity)
      : capacity_(capacity), free_(capacity) {
    if (capacity == 0) {
      throw MessageQueueException("DequeBoundedMessageQueue: capacity must be >= 1");
    }
  }

  DequeBoundedMessageQueue(const DequeBoundedMessageQueue&) = delete;
  DequeBoundedMessageQueue& operator=(const DequeBoundedMessageQueue&) = delete;
  DequeBoundedMessageQueue(DequeBoundedMessageQueue&&) = delete;
  DequeBoundedMessageQueue& operator=(DequeBoundedMessageQueue&&) = delete;

  std::size_t Size() const noexcept override {
    std::lock_guard<std::mutex> lock(mutex_);
    return buffer_.size();
  }

  bool Empty() const noexcept override {
    std::lock_guard<std::mutex> lock(mutex_);
    return buffer_.empty();
  }

  void Close() override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (closed_) {
        return;
      }
      closed_ = true;
      buffer_.clear();
      used_ = 0;
      free_ = capacity_;
    }
    not_full_.notify_all();
    not_empty_.notify_all();
  }

 protected:
  bool CheckSendDeadlockPossibility() const noexcept override {
    if constexpr (ThreadCategory == ThreadAccessCategory::kSingleProducerSingleConsumer ||
                  ThreadCategory == ThreadAccessCategory::kMultipleProducerSingleConsumer) {
      if (!this->IsBothRole()) {
        return false;
      }
      std::lock_guard<std::mutex> lock(mutex_);
      return free_ == 0;
    } else {
      return false;
    }
  }

  bool CheckReadDeadlockPossibility() const noexcept override {
    if constexpr (ThreadCategory == ThreadAccessCategory::kSingleProducerSingleConsumer ||
                  ThreadCategory == ThreadAccessCategory::kSingleProducerMultipleConsumer) {
      if (!this->IsBothRole()) {
        return false;
      }
      std::lock_guard<std::mutex> lock(mutex_);
      return used_ == 0;
    } else {
      return false;
    }
  }

  void SyncAndOverflowPrework() override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_) {
      throw MessageQueueException("send on closed queue");
    }
    not_full_.wait(lock, [this] { return closed_ || free_ > 0; });
    if (closed_) {
      throw MessageQueueException("send on closed queue");
    }
    --free_;
  }

  bool TrySyncAndOverflowPrework() noexcept override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || free_ == 0) {
      return false;
    }
    --free_;
    return true;
  }

  void StoreMessage(const ValueType& message) override {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
      buffer_.push_back(message);
    } catch (...) {
      ++free_;
      not_full_.notify_one();
      throw;
    }
  }

  void StoreMessage(ValueType&& message) override {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
      buffer_.push_back(std::move(message));
    } catch (...) {
      ++free_;
      not_full_.notify_one();
      throw;
    }
  }

  void SendPostwork() override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++used_;
    }
    not_empty_.notify_one();
  }

  void SyncAndUnderflowPrework() override {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this] { return (closed_ && used_ == 0) || used_ > 0; });
    if (used_ == 0) {
      throw MessageQueueException("read on closed empty queue");
    }
    --used_;
  }

  bool TrySyncAndUnderflowPrework() noexcept override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (used_ == 0) {
      return false;
    }
    --used_;
    return true;
  }

  ValueType PopMessage() override {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
      ValueType message = std::move(buffer_.front());
      buffer_.pop_front();
      return message;
    } catch (...) {
      ++used_;
      not_empty_.notify_one();
      throw;
    }
  }

  void ReadPostwork() override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++free_;
    }
    not_full_.notify_one();
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable not_full_;
  std::condition_variable not_empty_;
  const std::size_t capacity_;
  std::size_t free_;
  std::size_t used_ = 0;
  Container buffer_;
  bool closed_ = false;
};

}  // namespace message_queue
