#include "pulse/runtime.hpp"

#include <deque>
#include <mutex>

namespace pulse {
namespace {

/// FIFO: a single global ready queue, consumed first-in first-out.
///
/// This is the reference point.  It gives up locality entirely (any worker can
/// pick up any task) and ignores critical paths, but it is simple and, with a
/// contention-free queue, has very low per-task overhead.
class FifoPolicy final : public SchedulingPolicy {
public:
    void on_ready(const TaskHandle& task, WorkerId /*producer*/) override {
        std::lock_guard<std::mutex> lk(mtx_);
        queue_.push_back(task);
    }

    TaskHandle try_acquire(WorkerId /*worker*/) override {
        std::lock_guard<std::mutex> lk(mtx_);
        if (queue_.empty()) return nullptr;
        TaskHandle t = queue_.front();
        queue_.pop_front();
        return t;
    }

    std::size_t size() const noexcept override {
        std::lock_guard<std::mutex> lk(mtx_);
        return queue_.size();
    }

    const char* name() const noexcept override { return "fifo"; }

private:
    mutable std::mutex mtx_;
    std::deque<TaskHandle> queue_;
};

} // namespace

std::shared_ptr<SchedulingPolicy> make_fifo_policy() {
    return std::make_shared<FifoPolicy>();
}

} // namespace pulse
