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
 private:
  void OverwriteSpace() {
    head_ = (head_ + 1ull) % capacity_;
  }

  void WaitNotEmptySignal() {
    sync_.WaitReadEnd([this] () { return IsEmpty(); });
  }

  void AcquireSendSpaceBlocking() {
    sync_.AcquireSendOverwriteOldest();
  }
  
  bool TryAcquireSendSpace() {
    return sync_.TryAcquireSendOverwriteOldest();
  }
 protected:
  void ResolveSendOverflowBlocking() override {
    if (IsFull()) {
      OverwriteSpace();
    }
  }

  bool ResolveSendOverflowTry() override {
    if (IsFull()) {
      OverwriteSpace();
    }
    return true;
  }

  void ResolveReadUnderflowBlocking() override {
    WaitNotEmptySignal();
  }

  bool ResolveReadUnderflowTry() override {
    return !IsEmpty();
  }
  
  void SyncAndOverflowPrework() override {
    AcquireSendSpaceBlocking();
    ResolveSendOverflowBlocking();
  }

  bool TrySyncAndOverflowPrework() override {
    if (!TryAcquireSendSpace()) {
      return false;
    }
    return ResolveSendOverflowTry();
  }
 public:
  explicit CircularDropOldestMessageQueue(std::size_t capacity)
    : ICircularMessageQueue<ValueType, ThreadCategory, ExceptionPolicy, Allocator>(capacity) {}
};

}  // namespace message_queue