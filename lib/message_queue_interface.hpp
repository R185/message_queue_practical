#pragma once

#include <type_traits>
#include <optional>

#include "exception.hpp"

namespace message_queue {

template<typename T>
concept MessageType = std::is_move_constructible_v<T> && std::is_copy_constructible_v<T>;

enum class ThreadAccessCategory {
  kSingleProducerSingleConsumer,
  kMultipleProducerSingleConsumer,
  kSingleProducerMultipleConsume,
  kMultipleProducerMultipleConsumer
};

enum class DeadlockExceptionPolicy {
  kOnThreadRoleChange,
  kOnDeadlock,
  kNoException
};

template<
  MessageType ValueType,
  ThreadAccessCategory ThreadCategory,
  DeadlockExceptionPolicy ExceptionPolicy  
>
class IMessageQueue {
 private:
  enum class ThreadRole {
    kProducer,
    kConsumer,
    kBoth,
    kNoInfo
  };

  static ThreadRole& CurrentThreadRole() {
    thread_local ThreadRole role = ThreadRole::kNoInfo;
    return role;
  }

  void AccountThreads(ThreadRole role) {
    ThreadRole& current = CurrentThreadRole();
    if (current == ThreadRole::kNoInfo) {
      current = role;
      return;
    }

    if (current != role) {
      if constexpr (ExceptionPolicy == DeadlockExceptionPolicy::kOnThreadRoleChange) {
        throw MessageQueueException("Thread role is changed");
      }
      current = ThreadRole::kBoth;
    }
  }

  void HandleDeadlock() {
    if constexpr (ExceptionPolicy != DeadlockExceptionPolicy::kNoException) {
      throw MessageQueueException("Deadlock detected");
    }
  }

  void SendPrework() {
    AccountThreads(ThreadRole::kProducer);
    if (CheckSendDeadlockPossibility()) {
      HandleDeadlock();
    }
    SyncAndOverflowPrework();
  }

  bool TrySendPrework() {
    AccountThreads(ThreadRole::kProducer);
    if (CheckSendDeadlockPossibility()) {
      return false;
    }
    return TrySyncAndOverflowPrework();
  }

 protected:
  ThreadRole GetThreadRole() const noexcept {
    return CurrentThreadRole();
  }

  virtual bool CheckSendDeadlockPossibility() const noexcept = 0;
  virtual void SyncAndOverflowPrework() = 0;
  virtual bool TrySyncAndOverflowPrework() = 0;
  virtual void StoreMessage(const ValueType& message) = 0;
  virtual void StoreMessage(ValueType&& message) = 0;
  virtual void SendPostwork() {}

 public:
  IMessageQueue() = default;

  void Send(ValueType&& message) {
    SendPrework();
    StoreMessage(std::move(message));
    SendPostwork();
  }
  void Send(const ValueType& message) {
    SendPrework();
    StoreMessage(message);
    SendPostwork();
  }

  bool TrySend(ValueType&& message) {
    if (!TrySendPrework()) {
      return false;
    }
    StoreMessage(std::move(message));
    SendPostwork();
    return true;
  }
  bool TrySend(const ValueType& message) {
    if (!TrySendPrework()) {
      return false;
    }
    StoreMessage(message);
    SendPostwork();
    return true;
  }

  ValueType Read();

  std::optional<ValueType> TryRead();

  virtual std::size_t Size() const noexcept = 0;
  virtual bool Empty() const noexcept = 0;
  virtual void Clear();

  virtual ~IMessageQueue() = default;
};

}  // namespace message_queue
