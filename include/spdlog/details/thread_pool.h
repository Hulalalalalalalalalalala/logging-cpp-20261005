// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#include <spdlog/details/log_msg_buffer.h>
#include <spdlog/details/mpmc_blocking_q.h>
#include <spdlog/details/os.h>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

SPDLOG_NAMESPACE_BEGIN
class async_logger;

namespace details {

using async_logger_ptr = std::shared_ptr<async_logger>;

enum class async_msg_type { log, flush, terminate };

// Async msg to move to/from the queue
// Movable only. should never be copied
struct async_msg : log_msg_buffer {
    async_msg_type msg_type{async_msg_type::log};
    async_logger_ptr worker_ptr;

    async_msg() = default;
    ~async_msg() = default;

    // should only be moved in or out of the queue..
    async_msg(const async_msg &) = delete;

// support for vs2013 move
#if defined(_MSC_VER) && _MSC_VER <= 1800
    async_msg(async_msg &&other)
        : log_msg_buffer(std::move(other)),
          msg_type(other.msg_type),
          worker_ptr(std::move(other.worker_ptr)) {}

    async_msg &operator=(async_msg &&other) {
        *static_cast<log_msg_buffer *>(this) = std::move(other);
        msg_type = other.msg_type;
        worker_ptr = std::move(other.worker_ptr);
        return *this;
    }
#else  // (_MSC_VER) && _MSC_VER <= 1800
    async_msg(async_msg &&) = default;
    async_msg &operator=(async_msg &&) = default;
#endif

    // construct from log_msg with given type
    async_msg(async_logger_ptr &&worker, async_msg_type the_type, const details::log_msg &m)
        : log_msg_buffer{m},
          msg_type{the_type},
          worker_ptr{std::move(worker)} {}

    async_msg(async_logger_ptr &&worker, async_msg_type the_type)
        : log_msg_buffer{},
          msg_type{the_type},
          worker_ptr{std::move(worker)} {}

    explicit async_msg(async_msg_type the_type)
        : async_msg{nullptr, the_type} {}
};

class SPDLOG_API thread_pool {
public:
    using item_type = async_msg;
    using q_type = details::mpmc_blocking_queue<item_type>;

    thread_pool(size_t q_max_items,
                size_t threads_n,
                std::function<void()> on_thread_start,
                std::function<void()> on_thread_stop);
    thread_pool(size_t q_max_items, size_t threads_n, std::function<void()> on_thread_start);
    thread_pool(size_t q_max_items, size_t threads_n);

    // message all threads to terminate gracefully and join them
    ~thread_pool();

    thread_pool(const thread_pool &) = delete;
    thread_pool &operator=(thread_pool &&) = delete;

    void post_log(async_logger_ptr &&worker_ptr,
                  const details::log_msg &msg,
                  async_overflow_policy overflow_policy);
    // Post a flush request for the given logger. The request is always
    // accepted (waiting for room in the queue if needed, regardless of the
    // given overflow policy) and is never evicted or merged once queued, so
    // every accepted request eventually flushes each of the logger's sinks
    // exactly once. Returning means the request was submitted, not that the
    // sinks were already flushed.
    void post_flush(async_logger_ptr &&worker_ptr, async_overflow_policy overflow_policy);
    size_t overrun_counter();
    void reset_overrun_counter();
    size_t discard_counter();
    void reset_discard_counter();
    size_t queue_size();

private:
    q_type q_;

    std::vector<std::thread> threads_;

    // Flush ordering: every dequeued item carries a monotonically increasing
    // index (see q_type::dequeue). A flush request must not start flushing
    // any sink before every item dequeued ahead of it has been fully
    // processed, even when several worker threads share this pool.
    // completed_watermark_ is the lowest dequeue index not yet completed;
    // completed_ahead_ holds completed indices at or above the watermark.
    std::mutex completion_mutex_;
    std::condition_variable completion_cv_;
    size_t completed_watermark_{0};
    std::unordered_set<size_t> completed_ahead_;

    void post_async_msg_(async_msg &&new_msg, async_overflow_policy overflow_policy);
    void worker_loop_();

    // mark the item with the given dequeue index as fully processed
    void mark_completed_(size_t item_index);

    // wait until every item dequeued before the given index was fully processed
    void wait_for_predecessors_(size_t item_index);

    // process next message in the queue
    // return true if this thread should still be active (while no terminate msg
    // was received)
    bool process_next_msg_();
};

}  // namespace details
SPDLOG_NAMESPACE_END

#ifdef SPDLOG_HEADER_ONLY
#include "thread_pool-inl.h"
#endif
