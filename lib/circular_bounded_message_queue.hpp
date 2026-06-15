#pragma once

#include "circular_message_queue.hpp"
#include <iterator>

namespace message_queue {

template<
  MessageType ValueType,
  ThreadAccessCategory ThreadCategory,
  DeadlockExceptionPolicy ExceptionPolicy,
  typename Allocator = std::allocator<ValueType>
>
class CircularBoundedMessageQueue 
    : public ICircularMessageQueue<ValueType, ThreadCategory, ExceptionPolicy, Allocator> {
 protected:
  void ResolveSendOverflowBlocking() override {
    sync_.WaitSendEnd([this] () { return IsFull(); });
  }

  bool ResolveSendOverflowTry() override {
    return !IsFull();
  }

  void ResolveReadUnderflowBlocking() override {
    sync_.WaitReadEnd([this] () { return IsEmpty(); });
  }

  bool ResolveReadUnderflowTry() override {
    return IsEmpty();
  }

  void SyncAndOverflowPrework() override {
    sync_.AcquireSendBoundedBlock();
    ResolveSendOverflowBlocking();
  }

  bool TrySyncAndOverflowPrework() override {
    if (!sync_.TryAcquireSendBounded()) {
      return false;
    }
    if (!ResolveSendOverflowTry()) {
      CancelSendOperation();
      return false;
    }
    return true;
  }
 public:
  explicit CircularBoundedMessageQueue(std::size_t capacity)
      : ICircularMessageQueue<ValueType, ThreadCategory, ExceptionPolicy, Allocator>(capacity) {}
  
}

}  // namespace message_queue
