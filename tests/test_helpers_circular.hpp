#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <optional>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "circular/circular_bounded_message_queue.hpp"
#include "circular/circular_overwrite_oldest_message_queue.hpp"

namespace message_queue::test {

using BoundedSpsc = CircularBoundedMessageQueue<
    int, ThreadAccessCategory::kSingleProducerSingleConsumer, DeadlockExceptionPolicy::kNoException>;
using BoundedMpsc = CircularBoundedMessageQueue<
    int, ThreadAccessCategory::kMultipleProducerSingleConsumer, DeadlockExceptionPolicy::kNoException>;
using BoundedSpmc = CircularBoundedMessageQueue<
    int, ThreadAccessCategory::kSingleProducerMultipleConsumer, DeadlockExceptionPolicy::kNoException>;
using BoundedMpmc = CircularBoundedMessageQueue<
    int, ThreadAccessCategory::kMultipleProducerMultipleConsumer, DeadlockExceptionPolicy::kNoException>;

using OverwriteSpsc = CircularDropOldestMessageQueue<
    int, ThreadAccessCategory::kSingleProducerSingleConsumer, DeadlockExceptionPolicy::kNoException>;
using OverwriteMpsc = CircularDropOldestMessageQueue<
    int, ThreadAccessCategory::kMultipleProducerSingleConsumer, DeadlockExceptionPolicy::kNoException>;
using OverwriteSpmc = CircularDropOldestMessageQueue<
    int, ThreadAccessCategory::kSingleProducerMultipleConsumer, DeadlockExceptionPolicy::kNoException>;
using OverwriteMpmc = CircularDropOldestMessageQueue<
    int, ThreadAccessCategory::kMultipleProducerMultipleConsumer, DeadlockExceptionPolicy::kNoException>;

template<DeadlockExceptionPolicy Policy>
using BoundedWithPolicy = CircularBoundedMessageQueue<
    int, ThreadAccessCategory::kSingleProducerSingleConsumer, Policy>;

template<DeadlockExceptionPolicy Policy>
using OverwriteWithPolicy = CircularDropOldestMessageQueue<
    int, ThreadAccessCategory::kSingleProducerSingleConsumer, Policy>;

}  // namespace message_queue::test
