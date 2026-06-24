#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <thread>

#include "message_queue_interface.hpp"
#include "michael_scott_queue.hpp"

TEST(MichaelScottTests, StringSendAndRead) {
    MichaelScottQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};

    EXPECT_EQ(que.TrySend("123"), 1);

    EXPECT_EQ(que.TryRead(), "123");
}

TEST(MichaelScottTests, IntSendAndRead) {
    MichaelScottQueue<int,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};

    EXPECT_NO_THROW(que.Send(5));

    EXPECT_EQ(que.Read(), 5);
}

TEST(MichaelScottTests, OutOfCapacity) {
    MichaelScottQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};

    EXPECT_EQ(que.TrySend("123"), 1);

    EXPECT_EQ(que.TrySend("321"), 0);
}

TEST(MichaelScottTests, ReadEmptyMichaelScottQueue) {
    MichaelScottQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};

    EXPECT_EQ(que.TryRead(), std::optional<std::string>{});
    
    EXPECT_THROW(que.TryRead().value(), std::bad_optional_access);

    EXPECT_TRUE(que.Empty());
}

TEST(MichaelScottMichaelScottQueue, SendClosedMichaelScottQueue) {
    MichaelScottQueue<int,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};
    
    que.Close();

    EXPECT_THROW(que.Send(1), message_queue::MessageQueueException); 
}

TEST(MichaelScottMichaelScottQueue, ReadEmptyClosedMichaelScottQueue) {
    MichaelScottQueue<int,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};
    
    que.Close();

    EXPECT_FALSE(que.TryRead().has_value()); 
    
    EXPECT_THROW(que.Read(), message_queue::MessageQueueException); 
}

TEST(MichaelScottTests, NotThrowDestructor) {
    MichaelScottQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{1};

    que.Send("123");
    
    EXPECT_NO_THROW(que.~MichaelScottQueue());
}

TEST(MichaelScottTests, SizeCheck) {
    MichaelScottQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{5};

    que.Send("123");
    que.Send("321");
    que.Send("456");
    
    EXPECT_EQ(que.Size(), 3);
}

TEST(MichaelScottTests, StatsCheck) {
    MichaelScottQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{5};

    que.Send("123");
    que.Send("321");
    que.Send("456");
    
    QueueStatsSnapshot q_stats = que.Stats();

    EXPECT_EQ(q_stats.cas_retries, 0);
    EXPECT_EQ(q_stats.successful_operations, 3);
}

TEST(MichaelScottTests, MichaelScottQueueClose) {
    MichaelScottQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{5};

    que.Send("123");
    que.Send("321");
    que.Send("456");
    
    que.Close();

    EXPECT_EQ(que.TrySend("789"), 0);
    EXPECT_EQ(que.TryRead().value(), "123");
    EXPECT_EQ(que.TryRead().value(), "321");
    EXPECT_EQ(que.TryRead().value(), "456");
    EXPECT_EQ(que.TryRead(), std::optional<std::string>{});
}

TEST(MichaelScottTests, MichaelScottQueueSendClose) {
    MichaelScottQueue<std::string,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{5};
    
    que.Close();

    EXPECT_EQ(que.TrySend("789"), 0);
    EXPECT_EQ(que.TryRead(), std::optional<std::string>{});
}

TEST(MichaelScottMichaelScottQueue, ProducerConsumer) {
    const int kNumMessages = 1000;
    MichaelScottQueue<int,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{10};
    std::vector<int> received;

    std::thread consumer([&]() {
        for (int i = 0; i < kNumMessages; ++i) {
            std::optional<int> val = que.Read();

            if (val.has_value()) {
                received.push_back(val.value());
            }
        }
    });

    std::thread producer([&]() {
        for (int i = 0; i < kNumMessages; ++i) {
            que.Send(i);
        }
    });

    producer.join();
    consumer.join();

    EXPECT_EQ(received.size(), kNumMessages);
    
    for (int i = 0; i < kNumMessages; ++i) {
        EXPECT_EQ(received[i], i);
    }

    QueueStatsSnapshot q_stats = que.Stats();

    EXPECT_EQ(q_stats.cas_retries, 0);
    EXPECT_EQ(q_stats.successful_operations, 2000);
}

TEST(MichaelScottMichaelScottQueue, OldHeadNewTail) {
    MichaelScottQueue<int,
        message_queue::ThreadAccessCategory::kSingleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que{2};

    const int kProducers = 16;
    const int kMessagesPerProducer = 500000;
    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};

    auto chaos_worker = [&]() {
        while (!start) {
            std::this_thread::yield();
        }

        for (int i = 0; i < kMessagesPerProducer; ++i) {
            que.TryEnqueue(i);
            
            que.TryDequeue();
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kProducers; ++i) {
        threads.emplace_back(chaos_worker);
    }

    start = true;
    for (auto& t : threads) {
        t.join();
    }
    
    SUCCEED();
}

TEST(MichaelScottTests, ContendedSendCausesRetries) {
    const int kProducers = 4;
    const int kMessagesPerProducer = 10000;
    const int kCapacity = 100000;

    MichaelScottQueue<int,
        message_queue::ThreadAccessCategory::kMultipleProducerSingleConsumer,
        message_queue::DeadlockExceptionPolicy::kNoException
    > que(kCapacity);

    std::vector<std::thread> threads;
    for (int i = 0; i < kProducers; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < kMessagesPerProducer; ++j) {
                que.Send(j);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    QueueStatsSnapshot stats = que.Stats();

    EXPECT_GT(stats.successful_operations, 0);

    EXPECT_GT(stats.cas_retries, 0);
}
