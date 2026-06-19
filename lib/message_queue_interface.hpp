#pragma once

#include <type_traits>
#include <optional>
#include <unordered_map>

#include "exception.hpp"

namespace message_queue {

template<typename T>
concept MessageType = std::is_move_constructible_v<T> && std::is_copy_constructible_v<T>;

enum class ThreadAccessCategory {
  kSingleProducerSingleConsumer,
  kMultipleProducerSingleConsumer,
  kSingleProducerMultipleConsumer,
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

  static auto& ThreadRolesByInstance() {
    thread_local std::unordered_map<const IMessageQueue*, ThreadRole> roles;
    return roles;
  }

  static void MoveThreadRole(const IMessageQueue* from, IMessageQueue* to) noexcept {
    auto& roles = ThreadRolesByInstance();
    const auto it = roles.find(from);
    if (it != roles.end()) {
      roles[to] = it->second;
      roles.erase(it);
    }
  }

  static void CopyThreadRole(const IMessageQueue* from, IMessageQueue* to) noexcept {
    auto& roles = ThreadRolesByInstance();
    const auto it = roles.find(from);
    if (it != roles.end()) {
      roles[to] = it->second;
    }
  }

  static void DeleteThreadRole(const IMessageQueue* instance) noexcept {
    ThreadRolesByInstance().erase(instance);
  }

  ThreadRole& CurrentThreadRole() {
    return ThreadRolesByInstance().try_emplace(this, ThreadRole::kNoInfo).first->second;
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

  void ReadPrework() {
    AccountThreads(ThreadRole::kConsumer);
    if (CheckReadDeadlockPossibility()) {
      HandleDeadlock();
    }
    SyncAndUnderflowPrework();
  }

  bool TryReadPrework() {
    AccountThreads(ThreadRole::kConsumer);
    if (CheckReadDeadlockPossibility()) {
      return false;
    }
    return TrySyncAndUnderflowPrework();
  }

 protected:
  ThreadRole GetThreadRole() const noexcept {
    const auto& roles = ThreadRolesByInstance();
    const auto it = roles.find(this);
    return it != roles.end() ? it->second : ThreadRole::kNoInfo;
  }

  bool IsBothRole() const noexcept {
    return GetThreadRole() == ThreadRole::kBoth;
  }

  virtual bool CheckSendDeadlockPossibility() const noexcept = 0;
  virtual void SyncAndOverflowPrework() = 0;
  virtual bool TrySyncAndOverflowPrework() noexcept = 0;
  virtual void StoreMessage(const ValueType& message) = 0;
  virtual void StoreMessage(ValueType&& message) = 0;
  virtual void SendPostwork() {}

  virtual bool CheckReadDeadlockPossibility() const noexcept = 0;
  virtual void SyncAndUnderflowPrework() = 0;
  virtual bool TrySyncAndUnderflowPrework() noexcept = 0;
  virtual ValueType PopMessage() = 0;
  virtual void ReadPostwork() {}

 public:
  IMessageQueue() = default;

  IMessageQueue(const IMessageQueue& other) {
    CopyThreadRole(&other, this);
  }

  IMessageQueue& operator=(const IMessageQueue& other) {
    if (this != &other) {
      DeleteThreadRole(this);
      CopyThreadRole(&other, this);
    }
    return *this;
  }

  IMessageQueue(IMessageQueue&& other) noexcept {
    MoveThreadRole(&other, this);
  }

  IMessageQueue& operator=(IMessageQueue&& other) noexcept {
    if (this != &other) {
      DeleteThreadRole(this);
      MoveThreadRole(&other, this);
    }
    return *this;
  }

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

  ValueType Read() {
    ReadPrework();
    ValueType message = PopMessage();
    ReadPostwork();
    return message;
  }

  std::optional<ValueType> TryRead() {
    if (!TryReadPrework()) {
      return std::nullopt;
    }
    ValueType message = PopMessage();
    ReadPostwork();
    return {message};
  }

  virtual std::size_t Size() const noexcept = 0;
  virtual bool Empty() const noexcept = 0;
  virtual void Close() = 0;

  virtual ~IMessageQueue() {
    DeleteThreadRole(this);
  }
};

}  // namespace message_queue
