#pragma once

#include <cstddef>
#include <gtest/gtest.h>

#include "deque_bounded_message_queue.hpp"

namespace message_queue::test {

using BoundedSpsc = DequeBoundedMessageQueue<
    int, ThreadAccessCategory::kSingleProducerSingleConsumer, DeadlockExceptionPolicy::kNoException>;
using BoundedMpsc = DequeBoundedMessageQueue<
    int, ThreadAccessCategory::kMultipleProducerSingleConsumer, DeadlockExceptionPolicy::kNoException>;
using BoundedSpmc = DequeBoundedMessageQueue<
    int, ThreadAccessCategory::kSingleProducerMultipleConsumer, DeadlockExceptionPolicy::kNoException>;
using BoundedMpmc = DequeBoundedMessageQueue<
    int, ThreadAccessCategory::kMultipleProducerMultipleConsumer, DeadlockExceptionPolicy::kNoException>;

template <DeadlockExceptionPolicy Policy>
using BoundedWithPolicy = DequeBoundedMessageQueue<
    int, ThreadAccessCategory::kSingleProducerSingleConsumer, Policy>;

}  // namespace message_queue::test
