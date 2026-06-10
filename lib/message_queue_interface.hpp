#pragma once

#include <type_traits>
#include <optional>

namespace message_queue {

template<typename T>
concept MessageType = std::is_move_constructible_v<T> && std::is_copy_constructible_v<T>;

enum class ThreadAccessCategory {
  kSingleProducerSingleConsumer,
  kMultipleProducerSingleConsumer,
  kSingleProducerMultipleConsume,
  kMultipleProducerMultipleConsumer
};

enum class ExceptionPolicy {
  kOnConsumerToProducer,
  kOnDeadlock,
  kNoException
}

template<
  MessageType ValueType,
  ThreadAccessCategory ThreadCategory,
  ExceptionPolicy ExceptionPolicy  
>
class IMessageQueue {
 private:
 protected:
 public:
  void Send(ValueType&& message);
  void Send(const ValueType& message);
  bool TrySend(ValueType&& message);
  bool TrySend(const ValueType& message);

  ValueType Read();
  std::optional<ValueType> TryRead();

  virtual Size() const noexcept = 0;
  virtual Clear();

  virtual ~IMessageQueue() = default;
};

}  // namespace message_queue