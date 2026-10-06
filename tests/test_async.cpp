#include "includes.h"
#include "spdlog/async.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "test_sink.h"

#define TEST_FILENAME "test_logs/async_test.log"

namespace {
// Sink that verifies the flush ordering guarantee: when flush_() runs, every
// message posted before the flush request must already have been processed.
class flush_order_sink_mt : public spdlog::sinks::base_sink<std::mutex> {
public:
    explicit flush_order_sink_mt(size_t expected_msgs)
        : expected_msgs_(expected_msgs) {}

    size_t msg_counter() { return msg_counter_; }
    size_t flush_counter() { return flush_counter_; }
    size_t order_violations() { return order_violations_; }

protected:
    void sink_it_(const spdlog::details::log_msg &) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        ++msg_counter_;
    }

    void flush_() override {
        if (msg_counter_ != expected_msgs_) {
            ++order_violations_;
        }
        ++flush_counter_;
    }

    size_t msg_counter_{0};
    size_t flush_counter_{0};
    size_t order_violations_{0};
    size_t expected_msgs_;
};
}  // namespace


TEST_CASE("async zero queue size", "[async]") {
    REQUIRE_THROWS_AS(spdlog::details::thread_pool(0, 1), spdlog::spdlog_ex);
}

TEST_CASE("basic async test ", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    size_t overrun_counter = 0;
    size_t queue_size = 128;
    size_t messages = 256;
    {
        auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
        auto logger = std::make_shared<spdlog::async_logger>("as", test_sink, tp,
                                                             spdlog::async_overflow_policy::block);
        for (size_t i = 0; i < messages; i++) {
            logger->info("Hello message #{}", i);
        }
        logger->flush();
        overrun_counter = tp->overrun_counter();
    }
    REQUIRE(test_sink->msg_counter() == messages);
    REQUIRE(test_sink->flush_counter() == 1);
    REQUIRE(overrun_counter == 0);
}

TEST_CASE("discard policy ", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    test_sink->set_delay(std::chrono::milliseconds(1));
    size_t queue_size = 4;
    size_t messages = 1024;

    auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", test_sink, tp, spdlog::async_overflow_policy::overrun_oldest);
    for (size_t i = 0; i < messages; i++) {
        logger->info("Hello message");
    }
    REQUIRE(test_sink->msg_counter() < messages);
    REQUIRE(tp->overrun_counter() > 0);
}

TEST_CASE("discard policy discard_new ", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    test_sink->set_delay(std::chrono::milliseconds(1));
    size_t queue_size = 4;
    size_t messages = 1024;

    auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", test_sink, tp, spdlog::async_overflow_policy::discard_new);
    for (size_t i = 0; i < messages; i++) {
        logger->info("Hello message");
    }
    REQUIRE(test_sink->msg_counter() < messages);
    REQUIRE(tp->discard_counter() > 0);
}

TEST_CASE("discard policy using factory ", "[async]") {
    size_t queue_size = 4;
    size_t messages = 1024;
    spdlog::init_thread_pool(queue_size, 1);

    auto logger = spdlog::create_async_nb<spdlog::sinks::test_sink_mt>("as2");
    auto test_sink = std::static_pointer_cast<spdlog::sinks::test_sink_mt>(logger->sinks()[0]);
    test_sink->set_delay(std::chrono::milliseconds(3));

    for (size_t i = 0; i < messages; i++) {
        logger->info("Hello message");
    }

    REQUIRE(test_sink->msg_counter() < messages);
    spdlog::drop_all();
}

TEST_CASE("flush", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    size_t queue_size = 256;
    size_t messages = 256;
    {
        auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
        auto logger = std::make_shared<spdlog::async_logger>("as", test_sink, tp,
                                                             spdlog::async_overflow_policy::block);
        for (size_t i = 0; i < messages; i++) {
            logger->info("Hello message #{}", i);
        }

        logger->flush();
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(250));
    REQUIRE(test_sink->msg_counter() == messages);
    REQUIRE(test_sink->flush_counter() == 1);
}

TEST_CASE("async periodic flush", "[async]") {
    auto logger = spdlog::create_async<spdlog::sinks::test_sink_mt>("as");
    auto test_sink = std::static_pointer_cast<spdlog::sinks::test_sink_mt>(logger->sinks()[0]);

    spdlog::flush_every(std::chrono::seconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(1700));
    REQUIRE(test_sink->flush_counter() == 1);
    spdlog::flush_every(std::chrono::seconds(0));
    spdlog::drop_all();
}

TEST_CASE("tp->wait_empty() ", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    test_sink->set_delay(std::chrono::milliseconds(5));
    size_t messages = 100;

    auto tp = std::make_shared<spdlog::details::thread_pool>(messages, 2);
    auto logger = std::make_shared<spdlog::async_logger>("as", test_sink, tp,
                                                         spdlog::async_overflow_policy::block);
    for (size_t i = 0; i < messages; i++) {
        logger->info("Hello message #{}", i);
    }
    logger->flush();
    tp.reset();

    REQUIRE(test_sink->msg_counter() == messages);
    REQUIRE(test_sink->flush_counter() == 1);
}

TEST_CASE("multi threads", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    size_t queue_size = 128;
    size_t messages = 256;
    size_t n_threads = 10;
    {
        auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
        auto logger = std::make_shared<spdlog::async_logger>("as", test_sink, tp,
                                                             spdlog::async_overflow_policy::block);

        std::vector<std::thread> threads;
        for (size_t i = 0; i < n_threads; i++) {
            threads.emplace_back([logger, messages] {
                for (size_t j = 0; j < messages; j++) {
                    logger->info("Hello message #{}", j);
                }
            });
            logger->flush();
        }

        for (auto &t : threads) {
            t.join();
        }
    }

    REQUIRE(test_sink->msg_counter() == messages * n_threads);
    REQUIRE(test_sink->flush_counter() == n_threads);
}

TEST_CASE("to_file", "[async]") {
    prepare_logdir();
    size_t messages = 1024;
    size_t tp_threads = 1;
    spdlog::filename_t filename = SPDLOG_FILENAME_T(TEST_FILENAME);
    {
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(filename, true);
        auto tp = std::make_shared<spdlog::details::thread_pool>(messages, tp_threads);
        auto logger =
            std::make_shared<spdlog::async_logger>("as", std::move(file_sink), std::move(tp));

        for (size_t j = 0; j < messages; j++) {
            logger->info("Hello message #{}", j);
        }
    }

    require_message_count(TEST_FILENAME, messages);
    auto contents = file_contents(TEST_FILENAME);
    using spdlog::details::os::default_eol;
    REQUIRE(ends_with(contents, spdlog::fmt_lib::format("Hello message #1023{}", default_eol)));
}

TEST_CASE("to_file multi-workers", "[async]") {
    prepare_logdir();
    size_t messages = 1024 * 10;
    size_t tp_threads = 10;
    spdlog::filename_t filename = SPDLOG_FILENAME_T(TEST_FILENAME);
    {
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(filename, true);
        auto tp = std::make_shared<spdlog::details::thread_pool>(messages, tp_threads);
        auto logger =
            std::make_shared<spdlog::async_logger>("as", std::move(file_sink), std::move(tp));

        for (size_t j = 0; j < messages; j++) {
            logger->info("Hello message #{}", j);
        }
    }
    require_message_count(TEST_FILENAME, messages);
}

#ifndef SPDLOG_NO_EXCEPTIONS
TEST_CASE("bad_tp", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    std::shared_ptr<spdlog::details::thread_pool> const empty_tp;
    auto logger = std::make_shared<spdlog::async_logger>("as", test_sink, empty_tp);
    logger->info("Please throw an exception");
    REQUIRE(test_sink->msg_counter() == 0);
}
#endif  // SPDLOG_NO_EXCEPTIONS

TEST_CASE("flush requests are never overrun by logs", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    test_sink->set_delay(std::chrono::milliseconds(100));
    size_t queue_size = 4;
    size_t n_flushes = 4;
    size_t n_logs = 16;

    auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", test_sink, tp, spdlog::async_overflow_policy::overrun_oldest);

    // keep the single worker busy so the queue can be filled deterministically
    logger->info("busy message");
    // fill the whole queue with flush requests
    for (size_t i = 0; i < n_flushes; i++) {
        logger->flush();
    }
    // the queue now holds flush requests only, so these logs must be
    // discarded rather than evicting any flush request
    for (size_t i = 0; i < n_logs; i++) {
        logger->info("Hello message");
    }
    auto overrun_counter = tp->overrun_counter();
    auto discard_counter = tp->discard_counter();
    tp.reset();

    REQUIRE(test_sink->flush_counter() == n_flushes);
    REQUIRE(test_sink->msg_counter() == 1);
    REQUIRE(overrun_counter == 0);
    REQUIRE(discard_counter == n_logs);
}

TEST_CASE("overrun_oldest evicts only the oldest log, keeping flushes", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    test_sink->set_delay(std::chrono::milliseconds(100));
    size_t queue_size = 2;

    auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", test_sink, tp, spdlog::async_overflow_policy::overrun_oldest);

    logger->info("busy message");  // keeps the worker busy while posting
    logger->info("evicted message");
    logger->flush();               // queue is now [log, flush]
    logger->info("Hello message");  // must evict the oldest log, not the flush
    auto overrun_counter = tp->overrun_counter();
    auto discard_counter = tp->discard_counter();
    tp.reset();

    REQUIRE(test_sink->flush_counter() == 1);
    REQUIRE(test_sink->msg_counter() == 2);
    REQUIRE(overrun_counter == 1);
    REQUIRE(discard_counter == 0);
}

TEST_CASE("flush waits for room with discard_new policy", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    test_sink->set_delay(std::chrono::milliseconds(100));
    size_t queue_size = 2;

    auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", test_sink, tp, spdlog::async_overflow_policy::discard_new);

    logger->info("busy message");  // keeps the worker busy while posting
    // make sure the worker has dequeued the busy message (it then sleeps in
    // the sink for 100ms), so the queue state below is deterministic
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    logger->info("Hello message #1");
    logger->info("Hello message #2");  // queue is now full
    logger->info("discarded message");
    // the queue is full, but the flush request must still be accepted once
    // room is available instead of being discarded
    logger->flush();
    auto overrun_counter = tp->overrun_counter();
    auto discard_counter = tp->discard_counter();
    tp.reset();

    REQUIRE(test_sink->flush_counter() == 1);
    REQUIRE(test_sink->msg_counter() == 3);
    REQUIRE(discard_counter == 1);
    REQUIRE(overrun_counter == 0);
}

TEST_CASE("flush waits for prior messages with multiple workers", "[async]") {
    size_t messages = 128;
    auto sink = std::make_shared<flush_order_sink_mt>(messages);
    auto tp = std::make_shared<spdlog::details::thread_pool>(64, 4);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", sink, tp, spdlog::async_overflow_policy::block);

    for (size_t i = 0; i < messages; i++) {
        logger->info("Hello message #{}", i);
    }
    logger->flush();
    tp.reset();

    REQUIRE(sink->msg_counter() == messages);
    REQUIRE(sink->flush_counter() == 1);
    REQUIRE(sink->order_violations() == 0);
}

TEST_CASE("flush observes prior messages of other loggers sharing the pool", "[async]") {
    size_t messages = 64;
    auto sink = std::make_shared<flush_order_sink_mt>(messages);
    auto tp = std::make_shared<spdlog::details::thread_pool>(64, 4);
    auto logger_a = std::make_shared<spdlog::async_logger>(
        "as_a", sink, tp, spdlog::async_overflow_policy::block);
    auto logger_b = std::make_shared<spdlog::async_logger>(
        "as_b", sink, tp, spdlog::async_overflow_policy::block);

    for (size_t i = 0; i < messages; i++) {
        logger_b->info("Hello message #{}", i);
    }
    logger_a->flush();  // must wait for logger_b's earlier messages too
    tp.reset();

    REQUIRE(sink->msg_counter() == messages);
    REQUIRE(sink->flush_counter() == 1);
    REQUIRE(sink->order_violations() == 0);
}

TEST_CASE("flush completes when logger released before pool", "[async]") {
    prepare_logdir();
    spdlog::filename_t filename = SPDLOG_FILENAME_T(TEST_FILENAME);
    {
        auto tp = std::make_shared<spdlog::details::thread_pool>(8, 2);
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(filename, true);
        {
            auto logger = std::make_shared<spdlog::async_logger>(
                "as", file_sink, tp, spdlog::async_overflow_policy::block);
            logger->info("Hello message");
            logger->flush();
        }  // release the logger first
        tp.reset();  // releasing the pool must complete the accepted flush,
                     // without relying on the file sink's destructor
    }
    require_message_count(TEST_FILENAME, 1);
}

