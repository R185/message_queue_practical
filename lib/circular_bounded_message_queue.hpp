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
 private:
  void WaitNotFullSignal() {
    sync_.WaitSendEnd([this] () { return IsFull(); });
  }
  void WaitNotEmptySignal() {
    sync_.WaitReadEnd([this] () { return IsEmpty(); });
  }

  void AcquireSendSpaceBlocking() {
    sync_.AcquireSendBoundedBlock();
  }
  bool TryAcquireSendSpace() {
    return sync_.TryAcquireSendBounded();
  }
 protected:
  void ResolveSendOverflowBlocking() override {
    WaitNotFullSignal();
  }

  bool ResolveSendOverflowTry() override {
    return !IsFull();
  }

  void ResolveReadUnderflowBlocking() override {
    WaitNotEmptySignal();
  }

  bool ResolveReadUnderflowTry() override {
    return IsEmpty();
  }

  void SyncAndOverflowPrework() override {
    AcquireSendSpaceBlocking();
    ResolveSendOverflowBlocking();
  }

  bool TrySyncAndOverflowPrework() override {
    if (!TryAcquireSendSpace()) {
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
