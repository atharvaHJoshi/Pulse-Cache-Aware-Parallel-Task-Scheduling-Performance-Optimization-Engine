#include "pulse/dag.hpp"

#include <algorithm>
#include <queue>
#include <stdexcept>

namespace pulse {

TaskHandle DAG::create_task(std::function<void()> func, std::uint64_t cost_ns) {
    auto task = std::make_shared<Task>();
    task->id = next_id_++;
    task->func = std::move(func);
    task->cost_ns = cost_ns;
    tasks_.push_back(task);
    task_map_.emplace(task->id, task);
    return task;
}

void DAG::add_dependency(TaskId from, TaskId to) {
    if (from >= tasks_.size() || to >= tasks_.size()) {
        throw std::out_of_range("DAG::add_dependency: task id out of range");
    }
    tasks_[to]->deps.push_back(from);
    ++edge_count_;
}

void DAG::add_data_access(TaskId tid, DataId did, std::size_t bytes, bool write) {
    if (tid >= tasks_.size()) {
        throw std::out_of_range("DAG::add_data_access: task id out of range");
    }
    tasks_[tid]->data_accesses.push_back(DataAccess{did, bytes, write});
}

TaskHandle DAG::get_task(TaskId id) const {
    auto it = task_map_.find(id);
    return it == task_map_.end() ? nullptr : it->second;
}

void DAG::finalize() {
    const std::size_t n = tasks_.size();
    successors_.assign(n, {});
    predecessors_.assign(n, {});

    for (auto& t : tasks_) {
        t->dep_count.store(static_cast<int>(t->deps.size()), std::memory_order_relaxed);
        t->state.store(t->deps.empty() ? TaskState::Ready : TaskState::Pending,
                       std::memory_order_relaxed);
        for (TaskId dep : t->deps) {
            successors_[dep].push_back(t->id);
            predecessors_[t->id].push_back(dep);
        }
    }

    // Kahn topological sort; also detects cycles.
    std::vector<int> indeg(n, 0);
    std::queue<TaskId> q;
    for (std::size_t i = 0; i < n; ++i) {
        indeg[i] = static_cast<int>(predecessors_[i].size());
        if (indeg[i] == 0) q.push(i);
    }
    std::vector<TaskId> topo;
    topo.reserve(n);
    while (!q.empty()) {
        TaskId u = q.front();
        q.pop();
        topo.push_back(u);
        for (TaskId v : successors_[u]) {
            if (--indeg[v] == 0) q.push(v);
        }
    }
    if (topo.size() != n) {
        throw std::invalid_argument("DAG::finalize: graph contains a cycle");
    }

    // Bottom level: longest weighted path from a node to any sink (inclusive).
    for (auto it = topo.rbegin(); it != topo.rend(); ++it) {
        TaskId u = *it;
        std::uint64_t best = 0;
        for (TaskId s : successors_[u]) {
            best = std::max(best, tasks_[s]->critical_path_len);
        }
        tasks_[u]->critical_path_len = best + tasks_[u]->cost_ns;
    }

    total_critical_path_ = 0;
    for (std::size_t i = 0; i < n; ++i) {
        if (predecessors_[i].empty()) {
            total_critical_path_ = std::max(total_critical_path_, tasks_[i]->critical_path_len);
        }
    }
}

void DAG::reset_state() {
    for (auto& t : tasks_) {
        t->dep_count.store(static_cast<int>(t->deps.size()), std::memory_order_relaxed);
        t->state.store(t->deps.empty() ? TaskState::Ready : TaskState::Pending,
                       std::memory_order_relaxed);
        t->ready_ns = 0;
        t->start_ns = 0;
        t->end_ns = 0;
        t->worker = 0;
    }
}

} // namespace pulse
