#include "pulse/runtime.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <thread>

namespace pulse {

Runtime::Runtime(std::size_t workers, std::shared_ptr<SchedulingPolicy> policy)
    : workers_(workers == 0 ? std::max(1u, std::thread::hardware_concurrency()) : workers),
      policy_(std::move(policy)) {}

Runtime::~Runtime() {
    cancel_.store(true, std::memory_order_release);
    wake_cv_.notify_all();
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
}

std::uint64_t Runtime::now_ns() {
    using namespace std::chrono;
    return static_cast<std::uint64_t>(
        duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
}

RunResult Runtime::run(DAG& dag) {
    dag_ = &dag;
    dag.reset_state();
    total_ = dag.size();
    completed_.store(0, std::memory_order_relaxed);
    cancel_.store(false, std::memory_order_relaxed);
    wakeups_.store(0, std::memory_order_relaxed);

    records_.assign(workers_, {});
    worker_busy_.assign(workers_, 0);
    worker_idle_.assign(workers_, 0);

    policy_->on_dag_ready(dag, workers_);

    const std::uint64_t t0 = now_ns();

    // Publish source tasks *before* starting the workers.  This matters for
    // locality: if workers are already spinning when the first tasks arrive,
    // idle workers steal them from their intended owners before those owners
    // get a chance to run.  Publishing first lets placement actually be
    // honoured for the initial frontier.
    for (const auto& task : dag.tasks()) {
        if (task->deps.empty()) {
            make_ready(task, 0);
        }
    }

    threads_.clear();
    threads_.reserve(workers_);
    for (WorkerId i = 0; i < workers_; ++i) {
        threads_.emplace_back([this, i] { worker_loop(i); });
    }
    wake_cv_.notify_all();

    // Wait for completion.
    {
        std::unique_lock<std::mutex> lk(wake_mtx_);
        wake_cv_.wait(lk, [&] {
            return completed_.load(std::memory_order_acquire) >= total_ || cancel_.load();
        });
    }
    wake_cv_.notify_all();
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();

    RunResult result;
    result.policy = policy_->name();
    result.workers = workers_;
    result.tasks = total_;
    result.worker_busy_ns = worker_busy_;
    result.worker_idle_ns = worker_idle_;
    result.steals = policy_->steals();
    result.conflict_migrations = policy_->migrations();
    result.records.reserve(total_);

    std::uint64_t first = std::numeric_limits<std::uint64_t>::max();
    std::uint64_t last = 0;
    std::uint64_t work = 0;
    for (auto& v : records_) {
        for (auto& r : v) {
            first = std::min(first, r.start_ns);
            last = std::max(last, r.end_ns);
            work += r.run_ns();
            result.records.push_back(r);
        }
    }
    if (first == std::numeric_limits<std::uint64_t>::max()) first = t0;
    result.makespan_ns = last > first ? last - first : 0;
    result.total_work_ns = work;
    return result;
}

void Runtime::worker_loop(WorkerId id) {
    while (!cancel_.load(std::memory_order_relaxed) &&
           completed_.load(std::memory_order_acquire) < total_) {
        TaskHandle task = policy_->try_acquire(id);
        if (task) {
            execute(id, task);
            continue;
        }

        const std::uint64_t idle_start = now_ns();
        std::unique_lock<std::mutex> lk(wake_mtx_);
        if (cancel_.load(std::memory_order_relaxed) ||
            completed_.load(std::memory_order_acquire) >= total_) {
            worker_idle_[id] += now_ns() - idle_start;
            break;
        }
        wake_cv_.wait_for(lk, std::chrono::milliseconds(2), [&] {
            return cancel_.load(std::memory_order_relaxed) ||
                   completed_.load(std::memory_order_acquire) >= total_ ||
                   wakeups_.load(std::memory_order_acquire) > 0;
        });
        worker_idle_[id] += now_ns() - idle_start;

        const std::size_t pending = wakeups_.load(std::memory_order_acquire);
        if (pending > 0) {
            wakeups_.fetch_sub(1, std::memory_order_acq_rel);
        }
    }
}

void Runtime::execute(WorkerId id, const TaskHandle& task) {
    task->state.store(TaskState::Running, std::memory_order_relaxed);
    const std::uint64_t start = now_ns();
    task->start_ns = start;
    task->worker = id;
    policy_->on_start(id, task);

    bool ok = true;
    try {
        if (task->func) task->func();
    } catch (...) {
        ok = false;
    }

    const std::uint64_t end = now_ns();
    task->end_ns = end;
    task->state.store(ok ? TaskState::Completed : TaskState::Failed, std::memory_order_release);
    policy_->on_finish(id, task);

    records_[id].push_back(TaskRecord{task->id, id, task->ready_ns, start, end});
    worker_busy_[id] += end - start;

    const auto& succ = dag_->successors()[task->id];
    for (TaskId child_id : succ) {
        TaskHandle child = dag_->get_task(child_id);
        if (!child) continue;
        int remaining = child->dep_count.fetch_sub(1, std::memory_order_acq_rel) - 1;
        if (remaining == 0) {
            make_ready(child, id);
        }
    }

    completed_.fetch_add(1, std::memory_order_release);
    if (completed_.load(std::memory_order_acquire) >= total_) {
        wake_cv_.notify_all();
    } else {
        wake_cv_.notify_one();
    }
}

void Runtime::make_ready(const TaskHandle& task, WorkerId producer) {
    {
        std::lock_guard<std::mutex> lk(wake_mtx_);
        task->ready_ns = now_ns();
        task->state.store(TaskState::Ready, std::memory_order_relaxed);
        policy_->on_ready(task, producer);
        wakeups_.fetch_add(1, std::memory_order_release);
    }
    wake_cv_.notify_one();
}

} // namespace pulse
