#pragma once

#include "circular/circular_message_queue.hpp"

namespace message_queue {

template<
  MessageType ValueType,
  ThreadAccessCategory ThreadCategory,
  DeadlockExceptionPolicy ExceptionPolicy,
  typename Allocator = std::allocator<ValueType>>
class CircularDropOldestMessageQueue
    : public ICircularMessageQueue<ValueType, ThreadCategory, ExceptionPolicy, Allocator> {
 protected:
  void ResolveSendOverflowBlocking() {
    if (!this->IsFull()) {
      return;
    }
    if (!this->AdvanceHeadForOverwriteDrop()) {
      return;
    }
    this->CurrentSendGuard()->SuppressOccupiedNotify();
  }

  bool ResolveSendOverflowTry() {
    ResolveSendOverflowBlocking();
    return true;
  }

  void SyncAndOverflowPrework() override {
    this->CurrentSendGuard().emplace(this->sync_.AcquireSendOverwriteOldest());
    ResolveSendOverflowBlocking();
  }

  bool TrySyncAndOverflowPrework() noexcept override {
    std::optional<typename ICircularMessageQueue<
        ValueType, ThreadCategory, ExceptionPolicy, Allocator>::SendGuard> guard =
        this->sync_.TryAcquireSendOverwriteOldest();
    if (!guard.has_value()) {
      return false;
    }
    this->CurrentSendGuard().emplace(std::move(*guard));
    return ResolveSendOverflowTry();
  }

 public:
  explicit CircularDropOldestMessageQueue(std::size_t capacity)
      : ICircularMessageQueue<ValueType, ThreadCategory, ExceptionPolicy, Allocator>(capacity) {}

  ~CircularDropOldestMessageQueue() override = default;
};

}  // namespace message_queue
