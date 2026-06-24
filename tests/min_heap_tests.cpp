#include <gtest/gtest.h>
#include <string>

#include "exception.hpp"
#include "message_queue_interface.hpp"
#include "min-heap_queue.hpp"

TEST(MinHeapTests, StringTrySendAndTryRead) {
    message_queue::PriorityMessageQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};

    EXPECT_EQ(que.TrySend("123"), 1);

    EXPECT_EQ(que.TryRead().value(), "123");
}

TEST(MinHeapTests, StringSendAndRead) {
    message_queue::PriorityMessageQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};

    EXPECT_NO_THROW(que.Send("123"));

    EXPECT_EQ(que.Read(), "123");
}

TEST(MinHeapTests, ConstReferenceStringSendAndRead) {
    message_queue::PriorityMessageQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};

    std::string str = "123";

    const std::string &ref = str;

    EXPECT_NO_THROW(que.Send(ref));

    EXPECT_EQ(que.Read(), "123");

    EXPECT_NO_THROW(que.TrySend(ref));

    EXPECT_EQ(que.TryRead().value(), "123");
}

TEST(MinHeapTests, States) {
    message_queue::PriorityMessageQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};

    EXPECT_TRUE(que.Empty());

    EXPECT_EQ(que.Size(), 0);

    EXPECT_NO_THROW(que.Close());
}

TEST(MinHeapTests, SendAndReadFromEmptyAndClosedQueue) {
    message_queue::PriorityMessageQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};

    EXPECT_FALSE(que.TryRead().has_value());

    que.Close();

    EXPECT_THROW(que.Read(), message_queue::MessageQueueException);

    EXPECT_THROW(que.Send("123"), message_queue::MessageQueueException);
}

TEST(MinHeapTests, ZeroCapacity) {
    EXPECT_THROW((message_queue::PriorityMessageQueue<std::string,
                    message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
                    message_queue::DeadlockExceptionPolicy::kNoException
                >{0}), message_queue::MessageQueueException);
}

TEST(MinHeapTests, MultySend) {
    message_queue::PriorityMessageQueue<int,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{2};

    EXPECT_EQ(que.TrySend(1), 1);
    EXPECT_EQ(que.TrySend(2), 1);

    EXPECT_EQ(que.TryRead().value(), 1);
    EXPECT_EQ(que.TryRead().value(), 2);

    EXPECT_EQ(que.TrySend(2), 1);
    EXPECT_EQ(que.TrySend(1), 1);

    EXPECT_EQ(que.TryRead().value(), 1);
    EXPECT_EQ(que.TryRead().value(), 2);

    EXPECT_EQ(que.TrySend(1), 1);
    EXPECT_EQ(que.TrySend(1), 1);

    EXPECT_EQ(que.TryRead().value(), 1);
    EXPECT_EQ(que.TryRead().value(), 1);
}

TEST(MinHeapTests, OutOfCapacity) {
    message_queue::PriorityMessageQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};

    EXPECT_EQ(que.TrySend("123"), 1);
    EXPECT_EQ(que.TrySend("456"), 0);
}
