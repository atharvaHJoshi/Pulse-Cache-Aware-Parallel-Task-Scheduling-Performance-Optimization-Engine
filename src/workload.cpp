#include "pulse/workload.hpp"

#include <algorithm>
#include <atomic>
#include <random>
#include <vector>

namespace pulse {

namespace {

std::function<void()> spin_task(std::uint64_t cost_ns) {
    // Convert an estimated duration into a deterministic integer workload.
    // ~3 iterations/ns is a rough modern-CPU throughput for this loop; the
    // exact value does not matter, only that it is stable across schedulers.
    const std::uint64_t iters = std::max<std::uint64_t>(1, cost_ns * 3);
    return [iters]() {
        std::uint64_t x = 0x9e3779b97f4a7c15ULL;
        for (std::uint64_t i = 0; i < iters; ++i) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x += i;
        }
        static std::atomic<std::uint64_t> sink{0};
        sink.fetch_add(x, std::memory_order_relaxed);
    };
}

} // namespace

MemoryPool::MemoryPool(std::size_t regions, std::size_t region_bytes)
    : region_elems_(region_bytes / sizeof(double)) {
    regions_.resize(regions);
    for (auto& r : regions_) {
        r.assign(region_elems_, 1.0);
    }
}

double MemoryPool::touch(DataId id, std::size_t slice, std::size_t num_slices) const {
    if (id >= regions_.size() || num_slices == 0) return 0.0;
    const auto& r = regions_[id];
    const std::size_t per = std::max<std::size_t>(1, region_elems_ / num_slices);
    const std::size_t begin = std::min(region_elems_, slice * per);
    const std::size_t end = std::min(region_elems_, begin + per);
    double sum = 0.0;
    for (std::size_t i = begin; i < end; ++i) sum += r[i];
    return sum;
}

double MemoryPool::mutate(DataId id, std::size_t slot, std::size_t iters, double delta) {
    if (id >= regions_.size() || regions_[id].empty()) return 0.0;
    auto& r = regions_[id];
    const std::size_t i = slot % r.size();
    double sum = 0.0;
    for (std::size_t k = 0; k < iters; ++k) {
        r[i] += delta;
        sum += r[i];
    }
    return sum;
}

DAG build_chain(std::size_t n, std::uint64_t cost_ns) {
    DAG dag;
    TaskHandle prev = nullptr;
    for (std::size_t i = 0; i < n; ++i) {
        TaskHandle t = dag.create_task(spin_task(cost_ns), cost_ns);
        if (prev) dag.add_dependency(prev->id, t->id);
        prev = t;
    }
    dag.finalize();
    return dag;
}

DAG build_fork_join(std::size_t depth, std::uint64_t cost_ns) {
    DAG dag;
    std::vector<TaskHandle> level;
    TaskHandle root = dag.create_task(spin_task(cost_ns), cost_ns);
    level.push_back(root);
    for (std::size_t d = 1; d < depth; ++d) {
        std::vector<TaskHandle> next;
        for (auto& parent : level) {
            TaskHandle l = dag.create_task(spin_task(cost_ns), cost_ns);
            TaskHandle r = dag.create_task(spin_task(cost_ns), cost_ns);
            dag.add_dependency(parent->id, l->id);
            dag.add_dependency(parent->id, r->id);
            next.push_back(l);
            next.push_back(r);
        }
        level = std::move(next);
    }
    // Join sinks back into a single node to force a well-defined makespan.
    TaskHandle sink = dag.create_task(spin_task(cost_ns), cost_ns);
    for (auto& leaf : level) dag.add_dependency(leaf->id, sink->id);
    dag.finalize();
    return dag;
}

DAG build_layered(std::size_t levels, std::size_t width, std::uint64_t cost_ns,
                  std::uint64_t seed) {
    DAG dag;
    std::mt19937_64 rng(seed);
    std::vector<TaskHandle> prev;
    std::vector<TaskHandle> cur;
    for (std::size_t l = 0; l < levels; ++l) {
        cur.clear();
        for (std::size_t w = 0; w < width; ++w) {
            // Uneven costs so bottom-level prioritisation can matter.
            const std::uint64_t cost = cost_ns * (1 + (rng() % 4));
            cur.push_back(dag.create_task(spin_task(cost), cost));
        }
        if (!prev.empty()) {
            std::uniform_int_distribution<std::size_t> pick(0, prev.size() - 1);
            for (auto& t : cur) {
                std::size_t k = 1 + (rng() % 2);
                for (std::size_t j = 0; j < k; ++j) {
                    dag.add_dependency(prev[pick(rng)]->id, t->id);
                }
            }
        }
        prev = cur;
    }
    dag.finalize();
    return dag;
}

DAG build_random(std::size_t n, std::size_t avg_out, std::uint64_t cost_ns, std::uint64_t seed) {
    DAG dag;
    std::mt19937_64 rng(seed);
    std::vector<TaskHandle> tasks;
    tasks.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        const std::uint64_t cost = cost_ns * (1 + (rng() % 5));
        tasks.push_back(dag.create_task(spin_task(cost), cost));
    }
    std::uniform_int_distribution<std::size_t> pick(0, n - 1);
    for (std::size_t i = 0; i < n; ++i) {
        for (std::size_t e = 0; e < avg_out; ++e) {
            // Depend on an earlier task to keep the graph acyclic.
            if (i == 0) break;
            std::size_t j = pick(rng) % i;
            dag.add_dependency(tasks[j]->id, tasks[i]->id);
        }
    }
    dag.finalize();
    return dag;
}

DAG build_dataflow(std::size_t n, std::size_t regions, const std::shared_ptr<MemoryPool>& pool,
                   std::uint64_t seed) {
    DAG dag;
    std::mt19937_64 rng(seed);
    constexpr std::size_t kSlices = 8;
    constexpr std::uint64_t kCost = 2000;
    const std::size_t nregions =
        pool ? pool->region_count() : std::max<std::size_t>(1, regions);

    struct Owner {
        std::size_t last_task = 0;
        bool used = false;
    };
    std::vector<Owner> owners(nregions);

    std::uniform_int_distribution<std::size_t> region_pick(0, nregions - 1);
    std::uniform_int_distribution<std::size_t> slice_pick(0, kSlices - 1);

    for (std::size_t i = 0; i < n; ++i) {
        DataId rid = region_pick(rng);
        std::size_t slice = slice_pick(rng);
        std::shared_ptr<MemoryPool> p = pool;
        auto body = [p, rid, slice]() {
            if (p) {
                volatile double v = p->touch(rid, slice, 8);
                (void)v;
            }
            std::uint64_t x = rid + slice;
            for (int k = 0; k < 200; ++k) x = x * 6364136223846793005ULL + 1;
            static std::atomic<std::uint64_t> sink{0};
            sink.fetch_add(x, std::memory_order_relaxed);
        };
        TaskHandle t = dag.create_task(body, kCost);
        dag.add_data_access(t->id, rid, pool ? pool->region_elems() * sizeof(double) : 4096, false);

        // Dependency: share the region's previous producer (locality chain) and
        // occasionally a random earlier task (irregular parallelism).
        if (owners[rid].used) {
            dag.add_dependency(owners[rid].last_task, t->id);
        }
        if (i > 4 && (rng() % 4 == 0)) {
            std::size_t j = rng() % i;
            dag.add_dependency(j, t->id);
        }
        owners[rid].last_task = i;
        owners[rid].used = true;
    }
    dag.finalize();
    return dag;
}

DAG build_straggler(std::size_t chain_len, std::uint64_t chain_cost, std::size_t light_count,
                    std::uint64_t light_cost) {
    DAG dag;
    // Create the light, independent tasks first so a FIFO scheduler runs them
    // before discovering the heavy chain (the failure mode under test).
    std::vector<TaskHandle> light;
    light.reserve(light_count);
    for (std::size_t i = 0; i < light_count; ++i) {
        light.push_back(dag.create_task(spin_task(light_cost), light_cost));
    }
    TaskHandle prev = nullptr;
    for (std::size_t i = 0; i < chain_len; ++i) {
        TaskHandle t = dag.create_task(spin_task(chain_cost), chain_cost);
        if (prev) dag.add_dependency(prev->id, t->id);
        prev = t;
    }
    dag.finalize();
    return dag;
}

DAG build_contended(std::size_t regions, std::size_t tasks_per_region,
                    const std::shared_ptr<MemoryPool>& pool) {
    DAG dag;
    // Each task owns one distinct element (slot) but hammers it, so the cache
    // line holding that element (shared with seven neighbours) bounces between
    // cores whenever same-region tasks run apart.  `kKernel` sets how long each
    // task keeps the line hot.
    constexpr std::size_t kKernel = 256;
    constexpr std::uint64_t kCost = 2000;
    for (std::size_t r = 0; r < regions; ++r) {
        for (std::size_t j = 0; j < tasks_per_region; ++j) {
            const std::size_t slot = j;
            std::shared_ptr<MemoryPool> p = pool;
            auto body = [p, r, slot]() {
                if (p) {
                    volatile double v = p->mutate(r, slot, kKernel, 1.0);
                    (void)v;
                }
            };
            TaskHandle t = dag.create_task(body, kCost);
            dag.add_data_access(t->id, r, 4096, true);
        }
    }
    dag.finalize();
    return dag;
}

std::vector<Workload> standard_workloads() {
    std::vector<Workload> w;

    w.push_back({"chain-1k", "1,000-task sequential chain (critical-path bound)", [](std::uint64_t,
                                                                                     const std::shared_ptr<MemoryPool>&) {
                     return build_chain(1000, 3000);
                 }});

    w.push_back({"forkjoin-d10", "binary fork/join tree, depth 10 (~2,048 leaves)", [](std::uint64_t,
                                                                                       const std::shared_ptr<MemoryPool>&) {
                     return build_fork_join(10, 3000);
                 }});

    w.push_back({"layered-20x100", "20 layers x 100 tasks, random adjacent edges", [](std::uint64_t seed,
                                                                                      const std::shared_ptr<MemoryPool>&) {
                     return build_layered(20, 100, 3000, seed);
                 }});

    w.push_back({"random-5k-3", "5,000-task random DAG, avg out-degree 3", [](std::uint64_t seed,
                                                                              const std::shared_ptr<MemoryPool>&) {
                     return build_random(5000, 3, 3000, seed);
                 }});

    w.push_back({"dataflow-8k", "8,000-task memory-bound dataflow over 64 regions", [](std::uint64_t seed,
                                                                                       const std::shared_ptr<MemoryPool>& pool) {
                     return build_dataflow(8000, 64, pool, seed);
                 }});

    w.push_back({"straggler", "heavy critical chain (64 tasks) vs 4,000 cheap independent tasks", [](std::uint64_t,
                                                                                                      const std::shared_ptr<MemoryPool>&) {
                     return build_straggler(64, 60000, 4000, 3000);
                 }});

    w.push_back({"contended", "48 shared regions x 128 independent read-modify-write tasks", [](std::uint64_t,
                                                                                                 const std::shared_ptr<MemoryPool>& pool) {
                     return build_contended(48, 128, pool);
                 }});

    return w;
}

} // namespace pulse
