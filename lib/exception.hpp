#pragma once

#include <exception>

namespace message_queue {

class MessageQueueException final : public std::exception {
 private:
  const char* message_;
 public:
  MessageQueueException(const char* message)
    : message_(message) {}

  const char* what() const noexcept override {
    return message_;
  }
};

}  // namespace message_queue