#pragma once

#include <limits>
#include <semaphore>

namespace message_queue::circular::impl {

using QueueCountingSemaphore =
    std::counting_semaphore<std::numeric_limits<std::ptrdiff_t>::max()>;

}  // namespace message_queue::circular::impl
