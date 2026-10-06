// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#ifndef SPDLOG_HEADER_ONLY
#include <spdlog/details/thread_pool.h>
#endif

#include <cassert>
#include <spdlog/common.h>

SPDLOG_NAMESPACE_BEGIN
namespace details {

SPDLOG_INLINE thread_pool::thread_pool(size_t q_max_items,
                                       size_t threads_n,
                                       std::function<void()> on_thread_start,
                                       std::function<void()> on_thread_stop)
    : q_(q_max_items) {
    if (q_max_items == 0) {
        throw_spdlog_ex("thread_pool(): invalid q_max_items param (must be greater than 0)");
    }
    if (threads_n == 0 || threads_n > 1000) {
        throw_spdlog_ex(
            "thread_pool(): invalid threads_n param (valid "
            "range is 1-1000)");
    }
    for (size_t i = 0; i < threads_n; i++) {
        threads_.emplace_back([this, on_thread_start, on_thread_stop] {
            on_thread_start();
            this->thread_pool::worker_loop_();
            on_thread_stop();
        });
    }
}

SPDLOG_INLINE thread_pool::thread_pool(size_t q_max_items,
                                       size_t threads_n,
                                       std::function<void()> on_thread_start)
    : thread_pool(q_max_items, threads_n, std::move(on_thread_start), [] {}) {}

SPDLOG_INLINE thread_pool::thread_pool(size_t q_max_items, size_t threads_n)
    : thread_pool(q_max_items, threads_n, [] {}, [] {}) {}

// message all threads to terminate gracefully join them
SPDLOG_INLINE thread_pool::~thread_pool() {
    SPDLOG_TRY {
        for (size_t i = 0; i < threads_.size(); i++) {
            post_async_msg_(async_msg(async_msg_type::terminate), async_overflow_policy::block);
        }

        for (auto &t : threads_) {
            t.join();
        }
    }
    SPDLOG_CATCH_STD
}

void SPDLOG_INLINE thread_pool::post_log(async_logger_ptr &&worker_ptr,
                                         const details::log_msg &msg,
                                         async_overflow_policy overflow_policy) {
    async_msg async_m(std::move(worker_ptr), async_msg_type::log, msg);
    post_async_msg_(std::move(async_m), overflow_policy);
}

void SPDLOG_INLINE thread_pool::post_flush(async_logger_ptr &&worker_ptr,
                                           async_overflow_policy overflow_policy) {
    // An explicit flush request must always be accepted: wait for room if the
    // queue is full instead of overrunning existing messages or being
    // discarded, regardless of the logger's own overflow policy.
    (void)overflow_policy;
    post_async_msg_(async_msg(std::move(worker_ptr), async_msg_type::flush),
                    async_overflow_policy::block);
}

size_t SPDLOG_INLINE thread_pool::overrun_counter() { return q_.overrun_counter(); }

void SPDLOG_INLINE thread_pool::reset_overrun_counter() { q_.reset_overrun_counter(); }

size_t SPDLOG_INLINE thread_pool::discard_counter() { return q_.discard_counter(); }

void SPDLOG_INLINE thread_pool::reset_discard_counter() { q_.reset_discard_counter(); }

size_t SPDLOG_INLINE thread_pool::queue_size() { return q_.size(); }

void SPDLOG_INLINE thread_pool::post_async_msg_(async_msg &&new_msg,
                                                async_overflow_policy overflow_policy) {
    // Assign the sequence number and enqueue under the same lock, so that
    // sequence order always matches queue order.
    std::lock_guard<std::mutex> enqueue_lock(enqueue_mutex_);
    new_msg.seq = next_seq_++;
    if (overflow_policy == async_overflow_policy::block) {
        q_.enqueue(std::move(new_msg));
    } else if (overflow_policy == async_overflow_policy::overrun_oldest) {
        // Never overrun flush or terminate requests: only the oldest normal
        // log message may be dropped. If the full queue holds no normal log
        // (i.e. it contains only flush requests), the new message is
        // discarded and counted as such.
        const std::uint64_t seq = new_msg.seq;
        const bool pushed = q_.enqueue_nowait_over_oldest_if(
            std::move(new_msg),
            [](const async_msg &m) { return m.msg_type == async_msg_type::log; },
            [this](async_msg &&overrun) { mark_seq_complete_(overrun.seq); });
        if (!pushed) {
            mark_seq_complete_(seq);
        }
    } else {
        assert(overflow_policy == async_overflow_policy::discard_new);
        const std::uint64_t seq = new_msg.seq;
        if (!q_.enqueue_if_have_room(std::move(new_msg))) {
            mark_seq_complete_(seq);
        }
    }
}

void SPDLOG_INLINE thread_pool::mark_seq_complete_(std::uint64_t seq) {
    {
        std::lock_guard<std::mutex> lock(completion_mutex_);
        completed_ahead_.insert(seq);
        while (!completed_ahead_.empty() && *completed_ahead_.begin() == completed_through_ + 1) {
            completed_ahead_.erase(completed_ahead_.begin());
            ++completed_through_;
        }
    }
    completion_cv_.notify_all();
}

void SPDLOG_INLINE thread_pool::wait_seq_complete_(std::uint64_t seq) {
    std::unique_lock<std::mutex> lock(completion_mutex_);
    completion_cv_.wait(lock, [this, seq] { return completed_through_ + 1 >= seq; });
}

void SPDLOG_INLINE thread_pool::worker_loop_() {
    while (process_next_msg_()) {
    }
}

// process next message in the queue
// returns true if this thread should still be active (while no terminated msg was received)
bool SPDLOG_INLINE thread_pool::process_next_msg_() {
    async_msg incoming_async_msg;
    q_.dequeue(incoming_async_msg);
    const std::uint64_t seq = incoming_async_msg.seq;

    switch (incoming_async_msg.msg_type) {
        case async_msg_type::log: {
            incoming_async_msg.worker_ptr->backend_sink_it_(incoming_async_msg);
            mark_seq_complete_(seq);
            return true;
        }
        case async_msg_type::flush: {
            // Before flushing, wait until every message enqueued ahead of
            // this request (and not dropped by an overflow policy) has been
            // fully processed by its worker thread.
            wait_seq_complete_(seq);
            incoming_async_msg.worker_ptr->backend_flush_();
            mark_seq_complete_(seq);
            return true;
        }

        case async_msg_type::terminate: {
            mark_seq_complete_(seq);
            return false;
        }

        default: {
            assert(false);
        }
    }

    return true;
}

}  // namespace details
SPDLOG_NAMESPACE_END
