#pragma once

#include <cstddef>
#include <utility>
#include <vector>

#include "circular_queue_sync.hpp"
#include "message_queue_interface.hpp"

namespace message_queue {

template<
  MessageType ValueType,
  ThreadAccessCategory ThreadCategory,
  DeadlockExceptionPolicy ExceptionPolicy,
  typename Allocator = std::allocator<ValueType>
>
class ICircularMessageQueue
    : public IMessageQueue<ValueType, ThreadCategory, ExceptionPolicy> {
 private:
 protected:
  using Base = IMessageQueue<ValueType, ThreadCategory, ExceptionPolicy>;
  using Sync = CircularQueueSync<ThreadCategory>;

  Sync sync_;

  std::vector<ValueType, Allocator> buffer_;

  const std::size_t capacity_;

  std::size_t head_  = 0ull;
  std::size_t tail_  = 0ull;
  std::size_t count_ = 0ull;

 private:
  void AcquireReadOperation() {
    sync_.AcquireReadOperation();
  }

  bool TryAcquireReadOperation() {
    return sync_.TryAcquireReadOperation();
  }

  void CancelReadOperation() {
    sync_.CancelReadOperation();
  }

  ValueType ReadHead() {
    ValueType message = std::move(buffer_[head_]);
    head_ = (head_ + 1ull) % capacity_;
    return message;
  }

  void WriteTail(const ValueType& message) noexcept {
    buffer_[tail_] = message;
    tail_ = (tail_ + 1) % capacity_;
  }

  void WriteTail(ValueType&& message) {
    buffer_[tail_] = std::move(message);
    tail_ = (tail_ + 1) % capacity_;
  }

  void CompleatReadOperation() {
    sync_.ReleaseRead();
  }

  void CompleatSendOperation() {
    sync_.ReleaseSend();
  }

  void CancelSendOperation() {
    sync_.CancelSendOperation();
  }
 protected:
  virtual void ResolveReadUnderflowBlocking() = 0;
  virtual bool ResolveReadUnderflowTry() = 0;

  bool IsFull() const noexcept {
    return count_ >= capacity_;
  }
  bool IsEmpty() const noexcept {
    return count_ == 0;
  }

  bool CheckSendDeadlockPossibility() const noexcept override {
    if (IsBothRole()) {
      return sync_.ExecuteSynced([this] { return IsFull(); });
    } else {
      return false;
    }
  }

  bool CheckReadDeadlockPossibility() const noexcept override {
    if (IsBothRole()) {
      return sync_.ExecuteSynced([this] { return IsEmpty(); });
    } else {
      return false;
    }
  }

  void SyncAndUnderflowPrework() override {
    AcquireReadOperation();
    ResolveReadUnderflowBlocking();
  }

  bool TrySyncAndUnderflowPrework() override {
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
    --count_;
    ValueType message = ReadHead();
    CompleatReadOperation();
    return message;
  }

  void StoreMessage(const ValueType& message) noexcept override {
    WriteTail(message);
    ++count_;
    CompleatSendOperation();
  }

  void StoreMessage(ValueType&& message) override {
    WriteTail(std::move(message));
    ++count_;
    CompleatSendOperation();
  }  

  std::size_t Size() const noexcept override {
    return count_;
  }

  bool Empty() const noexcept override {
    return count_ == 0ull;
  }

  void Close() override {
    sync_.ExecuteSynced([this] {
      head_ = 0ull;
      tail_ = 0ull;
      count_ = 0ull;
      buffer_.clear();
    });
  }
 public:
  explicit ICircularMessageQueue(std::size_t capacity)
      : sync_(capacity)
      , buffer_(capacity)
      , capacity_(capacity) {}
};

}  // namespace message_queue