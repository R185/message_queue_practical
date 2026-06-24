#include <cstddef>
#include <optional>

#include <gtest/gtest.h>

#include "test_helpers_common.hpp"
#include "test_helpers_deque.hpp"

namespace message_queue::test {
namespace {

template <typename Queue>
void BecomeBothRoleOnSameThread(Queue& queue, std::size_t capacity) {
  FillQueue(queue, 0, static_cast<int>(capacity));
  EXPECT_EQ(queue.Read(), 0);
  FillQueue(queue, 1000, 1001);
  EXPECT_EQ(queue.Size(), capacity);
}

TEST(DequeBoundedDeadlock, OnDeadlockThrowsWhenBothRoleSendOnFull) {
  BoundedWithPolicy<DeadlockExceptionPolicy::kOnDeadlock> queue(kSmallCapacity);
  BecomeBothRoleOnSameThread(queue, kSmallCapacity);
  EXPECT_THROW(queue.Send(999), MessageQueueException);
}

TEST(DequeBoundedDeadlock, OnRoleChangeThrowsWhenSwitchingRoles) {
  BoundedWithPolicy<DeadlockExceptionPolicy::kOnThreadRoleChange> queue(kSmallCapacity);
  FillQueue(queue, 0, static_cast<int>(kSmallCapacity));
  EXPECT_THROW(queue.Read(), MessageQueueException);
}

TEST(DequeBoundedDeadlock, NoExceptTrySendReturnsFalseWhenBothRoleAndFull) {
  BoundedSpsc queue(kSmallCapacity);
  BecomeBothRoleOnSameThread(queue, kSmallCapacity);
  EXPECT_FALSE(queue.TrySend(999));
}

TEST(DequeBoundedDeadlock, OnDeadlockThrowsWhenBothRoleReadOnEmpty) {
  BoundedWithPolicy<DeadlockExceptionPolicy::kOnDeadlock> queue(kSmallCapacity);
  queue.Send(1);
  queue.Read();
  EXPECT_THROW(queue.Read(), MessageQueueException);
}

TEST(DequeBoundedDeadlock, NoExceptTryReadReturnsNulloptOnEmptyAfterBothRole) {
  BoundedSpsc queue(kSmallCapacity);
  queue.Send(1);
  queue.Read();
  EXPECT_EQ(queue.TryRead(), std::nullopt);
}

}  // namespace
}  // namespace message_queue::test
