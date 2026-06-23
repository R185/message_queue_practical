#pragma once

#include <atomic>
#include <cstddef>
#include <optional>

#include "circular/impl/semaphore.hpp"

namespace message_queue::circular::impl {

class MpmcBoundedSyncHelper {
 private:
  QueueCountingSemaphore free_space_;
  QueueCountingSemaphore occupied_space_;
  std::atomic<std::size_t> write_seq_{0};
  std::atomic<std::size_t> read_seq_{0};
  std::size_t capacity_;

  std::size_t ReserveWriteSlot() {
    return write_seq_.fetch_add(1, std::memory_order_acq_rel) % capacity_;
  }

 public:
  explicit MpmcBoundedSyncHelper(std::size_t capacity)
      : free_space_(static_cast<std::ptrdiff_t>(capacity))
      , occupied_space_(std::ptrdiff_t{0})
      , capacity_(capacity) {}

  std::size_t AcquireWriteSlotBlocking() {
    free_space_.acquire();
    return ReserveWriteSlot();
  }

  std::optional<std::size_t> TryAcquireWriteSlot() {
    if (!free_space_.try_acquire()) {
      return std::nullopt;
    }
    return ReserveWriteSlot();
  }

  bool TryAcquireWriteCredit() {
    return free_space_.try_acquire();
  }

  std::size_t AcquireReadSlotBlocking() {
    occupied_space_.acquire();
    return read_seq_.fetch_add(1, std::memory_order_acq_rel) % capacity_;
  }

  std::optional<std::size_t> TryAcquireReadSlot() {
    if (!occupied_space_.try_acquire()) {
      return std::nullopt;
    }
    return read_seq_.fetch_add(1, std::memory_order_acq_rel) % capacity_;
  }

  void AcquireReadCreditBlocking() {
    occupied_space_.acquire();
  }

  bool TryAcquireReadCredit() {
    return occupied_space_.try_acquire();
  }

  void CommitWrite(bool notify_occupied) {
    if (notify_occupied) {
      occupied_space_.release();
    }
  }

  void CancelWrite() {
    free_space_.release();
  }

  void CommitWriteCredit(bool notify_occupied) {
    if (notify_occupied) {
      occupied_space_.release();
    }
  }

  void CommitRead() {
    free_space_.release();
  }

  void CancelRead() {
    occupied_space_.release();
  }
};

}  // namespace message_queue::circular::impl
