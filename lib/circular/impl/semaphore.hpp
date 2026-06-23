#pragma once

#include <limits>
#include <semaphore>

namespace message_queue::circular::impl {

using QueueCountingSemaphore =
    std::counting_semaphore<std::numeric_limits<int>::max()>;

}  // namespace message_queue::circular::impl
