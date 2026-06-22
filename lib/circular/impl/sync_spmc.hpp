#pragma once

#include <concepts>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <utility>

#include "circular/impl/semaphore.hpp"

namespace message_queue {

template<>
class CircularQueueSync<ThreadAccessCategory::kSingleProducerMultipleConsumer> {
 public:
  class SendGuard {
   public:
    SendGuard() = default;
    SendGuard(const SendGuard&) = delete;
    SendGuard& operator=(const SendGuard&) = delete;
    SendGuard(SendGuard&& other) noexcept
        : sync_(other.sync_)
        , space_acquired_(other.space_acquired_)
        , notify_occupied_(other.notify_occupied_)
        , cancelled_(other.cancelled_) {
      other.sync_ = nullptr;
      other.space_acquired_ = false;
      other.notify_occupied_ = false;
      other.cancelled_ = false;
    }
    SendGuard& operator=(SendGuard&&) noexcept = delete;

    ~SendGuard() {
      if (!sync_) {
        return;
      }
      if (cancelled_) {
        if (space_acquired_) {
          sync_->free_space_.release();
        }
        return;
      }
      if (notify_occupied_) {
        sync_->occupied_space_.release();
      }
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
      return std::nullopt;
    }

   private:
    friend class CircularQueueSync;

    CircularQueueSync* sync_ = nullptr;
    bool space_acquired_ = false;
    bool notify_occupied_ = true;
    bool cancelled_ = false;

    explicit SendGuard(CircularQueueSync* sync, bool space_acquired, bool notify_occupied) noexcept
        : sync_(sync)
        , space_acquired_(space_acquired)
        , notify_occupied_(notify_occupied) {}
  };

  class ReadGuard {
   public:
    ReadGuard() = default;
    ReadGuard(const ReadGuard&) = delete;
    ReadGuard& operator=(const ReadGuard&) = delete;
    ReadGuard(ReadGuard&& other) noexcept
        : sync_(other.sync_)
        , space_acquired_(other.space_acquired_)
        , mutex_acquired_(other.mutex_acquired_)
        , cancelled_(other.cancelled_) {
      other.sync_ = nullptr;
      other.space_acquired_ = false;
      other.mutex_acquired_ = false;
      other.cancelled_ = false;
    }
    ReadGuard& operator=(ReadGuard&&) noexcept = delete;

    ~ReadGuard() {
      if (!sync_) {
        return;
      }
      if (cancelled_) {
        if (space_acquired_) {
          sync_->occupied_space_.release();
        }
        if (mutex_acquired_) {
          sync_->read_mutex_.unlock();
        }
        return;
      }
      if (space_acquired_) {
        sync_->free_space_.release();
      }
      if (mutex_acquired_) {
        sync_->read_mutex_.unlock();
      }
    }

    void Cancel() noexcept {
      cancelled_ = true;
    }

    template<std::invocable Func>
    void WaitReadEnd(Func&&) {}

    std::optional<std::size_t> ReservedIndex() const noexcept {
      return std::nullopt;
    }

   private:
    friend class CircularQueueSync;

    CircularQueueSync* sync_ = nullptr;
    bool space_acquired_ = false;
    bool mutex_acquired_ = false;
    bool cancelled_ = false;

    explicit ReadGuard(CircularQueueSync* sync, bool space_acquired, bool mutex_acquired) noexcept
        : sync_(sync)
        , space_acquired_(space_acquired)
        , mutex_acquired_(mutex_acquired) {}
  };

 private:
  std::mutex read_mutex_;
  circular::impl::QueueCountingSemaphore free_space_;
  circular::impl::QueueCountingSemaphore occupied_space_;

 public:
  explicit CircularQueueSync(std::size_t capacity)
      : free_space_(static_cast<std::ptrdiff_t>(capacity))
      , occupied_space_(std::ptrdiff_t{0}) {}

  ReadGuard AcquireReadOperation() {
    read_mutex_.lock();
    occupied_space_.acquire();
    return ReadGuard(this, true, true);
  }

  std::optional<ReadGuard> TryAcquireReadOperation() {
    if (!read_mutex_.try_lock()) {
      return std::nullopt;
    }
    if (!occupied_space_.try_acquire()) {
      read_mutex_.unlock();
      return std::nullopt;
    }
    return ReadGuard(this, true, true);
  }

  SendGuard AcquireSendBoundedBlock() {
    free_space_.acquire();
    return SendGuard(this, true, true);
  }

  std::optional<SendGuard> TryAcquireSendBounded() {
    if (!free_space_.try_acquire()) {
      return std::nullopt;
    }
    return SendGuard(this, true, true);
  }

  SendGuard AcquireSendOverwriteOldest() {
    const bool space_acquired = free_space_.try_acquire();
    return SendGuard(this, space_acquired, true);
  }

  std::optional<SendGuard> TryAcquireSendOverwriteOldest() {
    const bool space_acquired = free_space_.try_acquire();
    return SendGuard(this, space_acquired, true);
  }

  template<std::invocable Func>
  auto ExecuteSynced(Func&& func) const -> std::invoke_result_t<Func&> {
    return std::forward<Func>(func)();
  }

  void ConsumeOccupiedForOverwriteDrop() noexcept {
    occupied_space_.acquire();
  }
};

}  // namespace message_queue
