#pragma once

#include "circular/circular_message_queue.hpp"

namespace message_queue {

template<
  MessageType ValueType,
  ThreadAccessCategory ThreadCategory,
  DeadlockExceptionPolicy ExceptionPolicy,
  typename Allocator = std::allocator<ValueType>>
class CircularBoundedMessageQueue
    : public ICircularMessageQueue<ValueType, ThreadCategory, ExceptionPolicy, Allocator> {
 protected:
  void ResolveSendOverflowBlocking() {
    this->CurrentSendGuard()->WaitSendEnd([this] { return this->IsFull(); });
  }

  bool ResolveSendOverflowTry() {
    return !this->IsFull();
  }

  void SyncAndOverflowPrework() override {
    this->CurrentSendGuard().emplace(this->sync_.AcquireSendBoundedBlock());
    ResolveSendOverflowBlocking();
  }

  bool TrySyncAndOverflowPrework() noexcept override {
    std::optional<typename ICircularMessageQueue<
        ValueType, ThreadCategory, ExceptionPolicy, Allocator>::SendGuard> guard =
        this->sync_.TryAcquireSendBounded();
    if (!guard.has_value()) {
      return false;
    }
    this->CurrentSendGuard().emplace(std::move(*guard));
    if (!ResolveSendOverflowTry()) {
      this->CancelSendOperation();
      return false;
    }
    return true;
  }

 public:
  explicit CircularBoundedMessageQueue(std::size_t capacity)
      : ICircularMessageQueue<ValueType, ThreadCategory, ExceptionPolicy, Allocator>(capacity) {}

  ~CircularBoundedMessageQueue() override = default;
};

}  // namespace message_queue
