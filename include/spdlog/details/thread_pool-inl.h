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
    post_async_msg_(async_msg(std::move(worker_ptr), async_msg_type::flush), overflow_policy);
}

size_t SPDLOG_INLINE thread_pool::overrun_counter() { return q_.overrun_counter(); }

void SPDLOG_INLINE thread_pool::reset_overrun_counter() { q_.reset_overrun_counter(); }

size_t SPDLOG_INLINE thread_pool::discard_counter() { return q_.discard_counter(); }

void SPDLOG_INLINE thread_pool::reset_discard_counter() { q_.reset_discard_counter(); }

size_t SPDLOG_INLINE thread_pool::queue_size() { return q_.size(); }

void SPDLOG_INLINE thread_pool::post_async_msg_(async_msg &&new_msg,
                                                async_overflow_policy overflow_policy) {
    // Flush (and terminate) requests must always be accepted once submitted:
    // wait for room in the queue instead of evicting or discarding queued
    // messages, whatever overflow policy regular log messages use. The queue
    // capacity is never exceeded while waiting.
    if (new_msg.msg_type != async_msg_type::log ||
        overflow_policy == async_overflow_policy::block) {
        q_.enqueue(std::move(new_msg));
    } else if (overflow_policy == async_overflow_policy::overrun_oldest) {
        // Overrun the oldest regular log message only: queued flush requests
        // are never evicted and keep their relative order. If the queue holds
        // no regular log message, the new message is discarded instead.
        q_.enqueue_nowait(std::move(new_msg), [](const async_msg &queued_msg) {
            return queued_msg.msg_type == async_msg_type::log;
        });
    } else {
        assert(overflow_policy == async_overflow_policy::discard_new);
        q_.enqueue_if_have_room(std::move(new_msg));
    }
}

void SPDLOG_INLINE thread_pool::worker_loop_() {
    while (process_next_msg_()) {
    }
}

void SPDLOG_INLINE thread_pool::mark_completed_(size_t item_index) {
    bool advanced = false;
    {
        std::lock_guard<std::mutex> lock(completion_mutex_);
        if (item_index == completed_watermark_) {
            ++completed_watermark_;
            while (completed_ahead_.erase(completed_watermark_) != 0) {
                ++completed_watermark_;
            }
            advanced = true;
        } else {
            completed_ahead_.insert(item_index);
        }
    }
    if (advanced) {
        completion_cv_.notify_all();
    }
}

void SPDLOG_INLINE thread_pool::wait_for_predecessors_(size_t item_index) {
    std::unique_lock<std::mutex> lock(completion_mutex_);
    // completed_watermark_ is the lowest not-yet-completed dequeue index, so
    // every item ahead of item_index is done once it reaches item_index.
    completion_cv_.wait(lock, [this, item_index] { return item_index <= completed_watermark_; });
}

// process next message in the queue
// returns true if this thread should still be active (while no terminated msg was received)
bool SPDLOG_INLINE thread_pool::process_next_msg_() {
    async_msg incoming_async_msg;
    size_t item_index = 0;
    q_.dequeue(incoming_async_msg, item_index);

    // Mark the item as fully processed when leaving this scope, whatever
    // happens, so that a queued flush request never waits on an item that
    // otherwise would not be reported as completed.
    struct completion_guard {
        thread_pool &pool;
        size_t index;
        ~completion_guard() { pool.mark_completed_(index); }
    } guard{*this, item_index};

    switch (incoming_async_msg.msg_type) {
        case async_msg_type::log: {
            incoming_async_msg.worker_ptr->backend_sink_it_(incoming_async_msg);
            return true;
        }
        case async_msg_type::flush: {
            // A flush request must observe every message queued ahead of it
            // (and not dropped by the overflow policy) as fully processed
            // before it starts flushing any sink, even when several worker
            // threads share this pool.
            wait_for_predecessors_(item_index);
            incoming_async_msg.worker_ptr->backend_flush_();
            return true;
        }

        case async_msg_type::terminate: {
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
