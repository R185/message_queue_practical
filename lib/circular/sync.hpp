#pragma once

#include "message_queue_interface.hpp"

namespace message_queue {

template<ThreadAccessCategory Category>
class CircularQueueSync;

}  // namespace message_queue

#include "circular/impl/sync_spsc.hpp"
#include "circular/impl/sync_mpsc.hpp"
#include "circular/impl/sync_spmc.hpp"
#include "circular/impl/sync_mpmc.hpp"
