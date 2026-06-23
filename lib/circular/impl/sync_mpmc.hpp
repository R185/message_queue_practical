#pragma once

#include <atomic>
#include <concepts>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <utility>

#include "circular/impl/mpmc_bounded_sync_helper.hpp"

namespace message_queue {

template<>
class CircularQueueSync<ThreadAccessCategory::kMultipleProducerMultipleConsumer> {
 public:
  class SendGuard {
   public:
    SendGuard() = default;
    SendGuard(const SendGuard&) = delete;
    SendGuard& operator=(const SendGuard&) = delete;
    SendGuard(SendGuard&& other) noexcept
        : sync_(other.sync_)
        , mode_(other.mode_)
        , space_acquired_(other.space_acquired_)
        , notify_occupied_(other.notify_occupied_)
        , cancelled_(other.cancelled_)
        , reserved_index_(other.reserved_index_)
        , overwrite_lock_(std::move(other.overwrite_lock_)) {
      other.sync_ = nullptr;
      other.space_acquired_ = false;
      other.notify_occupied_ = false;
      other.cancelled_ = false;
      other.reserved_index_.reset();
    }
    SendGuard& operator=(SendGuard&&) noexcept = delete;

    ~SendGuard() {
      if (!sync_) {
        return;
      }
      if (mode_ == Mode::kBounded) {
        if (cancelled_) {
          if (space_acquired_) {
            sync_->ring_.CancelWrite();
          }
          return;
        }
        if (notify_occupied_ && space_acquired_) {
          sync_->ring_.CommitWrite(true);
        }
        return;
      }
      if (cancelled_) {
        if (space_acquired_) {
          sync_->ring_.CancelWrite();
        }
        return;
      }
      if (notify_occupied_) {
        sync_->ring_.CommitWriteCredit(true);
      }
      return;
    }

    void Cancel() noexcept {
      cancelled_ = true;
    }

    void SuppressOccupiedNotify() noexcept {
      notify_occupied_ = false;
    }

    template<std::invocable Func>
    void WaitSendEnd(Func&&) {}

    std::optional<std::size_t> ReservedIndex() const noexcept {
      return reserved_index_;
    }

   private:
    friend class CircularQueueSync;

    enum class Mode {
      kBounded,
      kOverwriteOldest
    };

    CircularQueueSync* sync_ = nullptr;
    Mode mode_ = Mode::kBounded;
    bool space_acquired_ = false;
    bool notify_occupied_ = false;
    bool cancelled_ = false;
    std::optional<std::size_t> reserved_index_;
    std::unique_lock<std::mutex> overwrite_lock_;

    explicit SendGuard(
        CircularQueueSync* sync,
        Mode mode,
        bool space_acquired,
        std::optional<std::size_t> reserved_index) noexcept
        : sync_(sync)
        , mode_(mode)
        , space_acquired_(space_acquired)
        , notify_occupied_(mode == Mode::kOverwriteOldest ? true : space_acquired)
        , reserved_index_(reserved_index) {}

    explicit SendGuard(
        CircularQueueSync* sync,
        Mode mode,
        bool space_acquired,
        std::optional<std::size_t> reserved_index,
        std::unique_lock<std::mutex>&& overwrite_lock) noexcept
        : sync_(sync)
        , mode_(mode)
        , space_acquired_(space_acquired)
        , notify_occupied_(mode == Mode::kOverwriteOldest ? true : space_acquired)
        , reserved_index_(reserved_index)
        , overwrite_lock_(std::move(overwrite_lock)) {}
  };

  class ReadGuard {
   public:
    ReadGuard() = default;
    ReadGuard(const ReadGuard&) = delete;
    ReadGuard& operator=(const ReadGuard&) = delete;
    ReadGuard(ReadGuard&& other) noexcept
        : sync_(other.sync_)
        , space_acquired_(other.space_acquired_)
        , cancelled_(other.cancelled_)
        , reserved_index_(other.reserved_index_) {
      other.sync_ = nullptr;
      other.space_acquired_ = false;
      other.cancelled_ = false;
      other.reserved_index_.reset();
    }
    ReadGuard& operator=(ReadGuard&&) noexcept = delete;

    ~ReadGuard() {
      if (!sync_) {
        return;
      }
      if (cancelled_) {
        if (space_acquired_) {
          sync_->ring_.CancelRead();
        }
        return;
      }
      if (space_acquired_) {
        sync_->ring_.CommitRead();
      }
    }

    void Cancel() noexcept {
      cancelled_ = true;
    }

    template<std::invocable Func>
    void WaitReadEnd(Func&&) {}

    std::optional<std::size_t> ReservedIndex() const noexcept {
      return reserved_index_;
    }

   private:
    friend class CircularQueueSync;

    CircularQueueSync* sync_ = nullptr;
    bool space_acquired_ = false;
    bool cancelled_ = false;
    std::optional<std::size_t> reserved_index_;

    explicit ReadGuard(
        CircularQueueSync* sync,
        bool space_acquired,
        std::optional<std::size_t> reserved_index) noexcept
        : sync_(sync)
        , space_acquired_(space_acquired)
        , reserved_index_(reserved_index) {}
  };

 private:
  mutable std::mutex execute_mutex_;
  std::mutex overwrite_mutex_;
  circular::impl::MpmcBoundedSyncHelper ring_;
  std::atomic<bool> overwrite_mode_{false};

 public:
  explicit CircularQueueSync(std::size_t capacity)
      : ring_(capacity) {}

  ReadGuard AcquireReadOperation() {
    if (overwrite_mode_.load(std::memory_order_acquire)) {
      ring_.AcquireReadCreditBlocking();
      return ReadGuard(this, true, std::nullopt);
    }
    return ReadGuard(this, true, ring_.AcquireReadSlotBlocking());
  }

  std::optional<ReadGuard> TryAcquireReadOperation() {
    if (overwrite_mode_.load(std::memory_order_acquire)) {
      if (!ring_.TryAcquireReadCredit()) {
        return std::nullopt;
      }
      return ReadGuard(this, true, std::nullopt);
    }
    const std::optional<std::size_t> index = ring_.TryAcquireReadSlot();
    if (!index.has_value()) {
      return std::nullopt;
    }
    return ReadGuard(this, true, *index);
  }

  SendGuard AcquireSendBoundedBlock() {
    overwrite_mode_.store(false, std::memory_order_release);
    return SendGuard(this, SendGuard::Mode::kBounded, true, ring_.AcquireWriteSlotBlocking());
  }

  std::optional<SendGuard> TryAcquireSendBounded() {
    overwrite_mode_.store(false, std::memory_order_release);
    const std::optional<std::size_t> index = ring_.TryAcquireWriteSlot();
    if (!index.has_value()) {
      return std::nullopt;
    }
    return SendGuard(this, SendGuard::Mode::kBounded, true, *index);
  }

  SendGuard AcquireSendOverwriteOldest() {
    overwrite_mode_.store(true, std::memory_order_release);
    std::unique_lock<std::mutex> lock(overwrite_mutex_);
    const bool space_acquired = ring_.TryAcquireWriteCredit();
    return SendGuard(
        this,
        SendGuard::Mode::kOverwriteOldest,
        space_acquired,
        std::nullopt,
        std::move(lock));
  }

  std::optional<SendGuard> TryAcquireSendOverwriteOldest() {
    overwrite_mode_.store(true, std::memory_order_release);
    std::unique_lock<std::mutex> lock(overwrite_mutex_, std::try_to_lock);
    if (!lock.owns_lock()) {
      return std::nullopt;
    }
    const bool space_acquired = ring_.TryAcquireWriteCredit();
    return SendGuard(
        this,
        SendGuard::Mode::kOverwriteOldest,
        space_acquired,
        std::nullopt,
        std::move(lock));
  }

  template<std::invocable Func>
  auto ExecuteSynced(Func&& func) const -> std::invoke_result_t<Func&> {
    std::lock_guard lock(execute_mutex_);
    return std::forward<Func>(func)();
  }
};

}  // namespace message_queue
