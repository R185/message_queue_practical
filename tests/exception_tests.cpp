#include <gtest/gtest.h>

#include "exception.hpp"

TEST(ExceptionTests, ThrowAndWhat) {
    message_queue::MessageQueueException excp{"some exception"};

    EXPECT_THROW(throw excp, message_queue::MessageQueueException);
    
    EXPECT_EQ(excp.what(), "some exception");
}
