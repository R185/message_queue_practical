#pragma once

#include <atomic>
#include <cstddef>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "circular/sync.hpp"
#include "message_queue_interface.hpp"

namespace message_queue {

template<
  MessageType ValueType,
  ThreadAccessCategory ThreadCategory,
  DeadlockExceptionPolicy ExceptionPolicy,
  typename Allocator = std::allocator<ValueType>>
class ICircularMessageQueue : public IMessageQueue<ValueType, ThreadCategory, ExceptionPolicy> {
 protected:
  using Base = IMessageQueue<ValueType, ThreadCategory, ExceptionPolicy>;
  using Sync = CircularQueueSync<ThreadCategory>;
  using SendGuard = typename Sync::SendGuard;
  using ReadGuard = typename Sync::ReadGuard;

  Sync sync_;
  std::vector<ValueType, Allocator> buffer_;
  const std::size_t capacity_;

  using HeadType = std::conditional_t<
      ThreadCategory == ThreadAccessCategory::kSingleProducerSingleConsumer ||
          ThreadCategory == ThreadAccessCategory::kMultipleProducerSingleConsumer,
      std::size_t,
      std::atomic<std::size_t>>;

  using TailType = std::conditional_t<
      ThreadCategory == ThreadAccessCategory::kSingleProducerSingleConsumer ||
          ThreadCategory == ThreadAccessCategory::kSingleProducerMultipleConsumer,
      std::size_t,
      std::atomic<std::size_t>>;

  std::size_t head_{0ull};
  std::size_t tail_{0ull};
  std::atomic<std::size_t> count_{0ull};

  static auto& SendGuardsByThread() {
    thread_local std::unordered_map<const ICircularMessageQueue*, std::optional<SendGuard>> guards;
    return guards;
  }

  static auto& ReadGuardsByThread() {
    thread_local std::unordered_map<const ICircularMessageQueue*, std::optional<ReadGuard>> guards;
    return guards;
  }

  std::optional<SendGuard>& CurrentSendGuard() {
    return SendGuardsByThread()[this];
  }

  std::optional<ReadGuard>& CurrentReadGuard() {
    return ReadGuardsByThread()[this];
  }

  std::size_t HeadIndex() const noexcept {
    if constexpr (std::is_same_v<HeadType, std::size_t>) {
      return head_;
    } else {
      return head_atomic_.load(std::memory_order_acquire);
    }
  }

  void SetHeadIndex(std::size_t value) noexcept {
    if constexpr (std::is_same_v<HeadType, std::size_t>) {
      head_ = value;
    } else {
      head_atomic_.store(value, std::memory_order_release);
    }
  }

  void CancelSendOperation() {
    auto& guard = CurrentSendGuard();
    if (guard.has_value()) {
      guard->Cancel();
      guard.reset();
    }
  }

 private:
  std::size_t TailIndex() const noexcept {
    if constexpr (std::is_same_v<TailType, std::size_t>) {
      return tail_;
    } else {
      return tail_atomic_.load(std::memory_order_acquire);
    }
  }

  void SetTailIndex(std::size_t value) noexcept {
    if constexpr (std::is_same_v<TailType, std::size_t>) {
      tail_ = value;
    } else {
      tail_atomic_.store(value, std::memory_order_release);
    }
  }

  void AcquireReadOperation() {
    CurrentReadGuard().emplace(sync_.AcquireReadOperation());
  }

  bool TryAcquireReadOperation() {
    std::optional<ReadGuard> guard = sync_.TryAcquireReadOperation();
    if (!guard.has_value()) {
      return false;
    }
    CurrentReadGuard().emplace(std::move(*guard));
    return true;
  }

  void CancelReadOperation() {
    auto& guard = CurrentReadGuard();
    if (guard.has_value()) {
      guard->Cancel();
      guard.reset();
    }
  }

  ValueType ReadHead() {
    std::size_t index = HeadIndex();
    auto& guard = CurrentReadGuard();
    if (guard.has_value()) {
      const std::optional<std::size_t> reserved = guard->ReservedIndex();
      if (reserved.has_value()) {
        index = *reserved;
      }
    }
    ValueType message = std::move(buffer_[index]);
    if (!(guard.has_value() && guard->ReservedIndex().has_value())) {
      SetHeadIndex((index + 1ull) % capacity_);
    }
    return message;
  }

  void WriteTail(const ValueType& message) noexcept {
    std::size_t index = TailIndex();
    auto& guard = CurrentSendGuard();
    if (guard.has_value()) {
      const std::optional<std::size_t> reserved = guard->ReservedIndex();
      if (reserved.has_value()) {
        index = *reserved;
      }
    }
    buffer_[index] = message;
    if (!(guard.has_value() && guard->ReservedIndex().has_value())) {
      SetTailIndex((index + 1ull) % capacity_);
    }
  }

  void WriteTail(ValueType&& message) {
    std::size_t index = TailIndex();
    auto& guard = CurrentSendGuard();
    if (guard.has_value()) {
      const std::optional<std::size_t> reserved = guard->ReservedIndex();
      if (reserved.has_value()) {
        index = *reserved;
      }
    }
    buffer_[index] = std::move(message);
    if (!(guard.has_value() && guard->ReservedIndex().has_value())) {
      SetTailIndex((index + 1ull) % capacity_);
    }
  }

  void CompleatReadOperation() {
    CurrentReadGuard().reset();
  }

  void CompleatSendOperation() {
    CurrentSendGuard().reset();
  }

 protected:
  void ResolveReadUnderflowBlocking() {
    auto& guard = CurrentReadGuard();
    if (guard.has_value()) {
      guard->WaitReadEnd([this] { return IsEmpty(); });
    }
  }

  bool ResolveReadUnderflowTry() {
    return !IsEmpty();
  }

  bool IsFull() const noexcept {
    return count_.load(std::memory_order_acquire) >= capacity_;
  }

  bool IsEmpty() const noexcept {
    return count_.load(std::memory_order_acquire) == 0;
  }

  bool CheckSendDeadlockPossibility() const noexcept override {
    if (!this->IsBothRole()) {
      return false;
    }
    return sync_.ExecuteSynced([this] { return IsFull(); });
  }

  void StoreMessage(const ValueType& message) noexcept override {
    WriteTail(message);
    count_.fetch_add(1, std::memory_order_release);
    CompleatSendOperation();
  }

  void StoreMessage(ValueType&& message) override {
    WriteTail(std::move(message));
    count_.fetch_add(1, std::memory_order_release);
    CompleatSendOperation();
  }

  bool CheckReadDeadlockPossibility() const noexcept override {
    if (!this->IsBothRole()) {
      return false;
    }
    return sync_.ExecuteSynced([this] { return IsEmpty(); });
  }

  void SyncAndUnderflowPrework() override {
    AcquireReadOperation();
    ResolveReadUnderflowBlocking();
  }

  bool TrySyncAndUnderflowPrework() noexcept override {
    if (!TryAcquireReadOperation()) {
      return false;
    }
    if (!ResolveReadUnderflowTry()) {
      CancelReadOperation();
      return false;
    }
    return true;
  }

  ValueType PopMessage() override {
    ValueType message = ReadHead();
    count_.fetch_sub(1, std::memory_order_release);
    CompleatReadOperation();
    return message;
  }

 public:
  explicit ICircularMessageQueue(std::size_t capacity)
      : sync_(capacity)
      , buffer_(capacity)
      , capacity_(capacity) {
    if constexpr (!std::is_same_v<HeadType, std::size_t>) {
      head_atomic_.store(0ull, std::memory_order_relaxed);
    }
    if constexpr (!std::is_same_v<TailType, std::size_t>) {
      tail_atomic_.store(0ull, std::memory_order_relaxed);
    }
  }

  ~ICircularMessageQueue() override = default;

  std::size_t Size() const noexcept override {
    return count_.load(std::memory_order_acquire);
  }

  bool Empty() const noexcept override {
    return count_.load(std::memory_order_acquire) == 0ull;
  }

  void Close() override {
    sync_.ExecuteSynced([this] {
      SetHeadIndex(0ull);
      SetTailIndex(0ull);
      count_.store(0ull, std::memory_order_release);
      buffer_.clear();
    });
  }

 private:
  std::atomic<std::size_t> head_atomic_{0ull};
  std::atomic<std::size_t> tail_atomic_{0ull};
};

}  // namespace message_queue
