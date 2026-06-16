#pragma once

#include "circular_message_queue.hpp"

namespace message_queue {

template<
  MessageType ValueType,
  ThreadAccessCategory ThreadCategory,
  DeadlockExceptionPolicy ExceptionPolicy,
  typename Allocator = std::allocator<ValueType>
>
class CircularDropOldestMessageQueue
  : public ICircularMessageQueue<ValueType, ThreadCategory, ExceptionPolicy, Allocator> {  
 protected:
  void ResolveSendOverflowBlocking() override {
    if (IsFull()) {
      head_ = (head_ + 1ull) % capacity_;
    }
  }

  bool ResolveSendOverflowTry() override {
    if (IsFull()) {
      head_ = (head_ + 1ull) % capacity_;
    }
    return true;
  }

  void SyncAndOverflowPrework() override {
    sync_.AcquireSendOverwriteOldest();
    ResolveSendOverflowBlocking();
  }

  bool TrySyncAndOverflowPrework() override {
    if (!sync_.TryAcquireSendOverwriteOldest()) {
      return false;
    }
    return ResolveSendOverflowTry();
  }
 public:
  explicit CircularDropOldestMessageQueue(std::size_t capacity)
    : ICircularMessageQueue<ValueType, ThreadCategory, ExceptionPolicy, Allocator>(capacity) {}
};

}  // namespace message_queue