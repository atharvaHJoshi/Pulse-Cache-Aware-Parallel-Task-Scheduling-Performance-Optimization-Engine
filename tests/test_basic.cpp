#include "pulse/dag.hpp"
#include "pulse/runtime.hpp"
#include "pulse/workload.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <vector>

using namespace pulse;

namespace {

std::vector<std::shared_ptr<SchedulingPolicy>> all_policies() {
    return {make_fifo_policy(), make_work_stealing_policy(), make_critical_path_policy(),
            make_locality_aware_policy()};
}

} // namespace

TEST(DagTest, RejectsCycles) {
    DAG dag;
    auto a = dag.create_task([] {});
    auto b = dag.create_task([] {});
    dag.add_dependency(a->id, b->id);
    dag.add_dependency(b->id, a->id);
    EXPECT_THROW(dag.finalize(), std::invalid_argument);
}

TEST(DagTest, CriticalPathOnChain) {
    DAG dag = build_chain(5, 100);
    EXPECT_EQ(dag.critical_path(), 500u);
    EXPECT_EQ(dag.tasks()[0]->critical_path_len, 500u); // source
    EXPECT_EQ(dag.tasks()[4]->critical_path_len, 100u); // sink
}

TEST(DagTest, BottomLevelOnDiamond) {
    //   a(1)
    //  /    \
    // b(2)  c(2)
    //  \    /
    //   d(3)
    DAG dag;
    auto a = dag.create_task([] {}, 1);
    auto b = dag.create_task([] {}, 2);
    auto c = dag.create_task([] {}, 2);
    auto d = dag.create_task([] {}, 3);
    dag.add_dependency(a->id, b->id);
    dag.add_dependency(a->id, c->id);
    dag.add_dependency(b->id, d->id);
    dag.add_dependency(c->id, d->id);
    dag.finalize();
    EXPECT_EQ(dag.tasks()[a->id]->critical_path_len, 1u + 2u + 3u);
    EXPECT_EQ(dag.tasks()[b->id]->critical_path_len, 2u + 3u);
    EXPECT_EQ(dag.tasks()[d->id]->critical_path_len, 3u);
    EXPECT_EQ(dag.critical_path(), 6u);
}

// Every task must run exactly once, and never before all of its dependencies.
TEST(RuntimeTest, RespectsDependencies) {
    for (auto& policy : all_policies()) {
        DAG dag;
        constexpr std::size_t N = 200;
        std::vector<TaskHandle> tasks;
        std::atomic<int> executed{0};
        std::vector<std::atomic<int>> completion_index(N);
        for (std::size_t i = 0; i < N; ++i) completion_index[i].store(-1);
        std::atomic<int> counter{0};

        for (std::size_t i = 0; i < N; ++i) {
            tasks.push_back(dag.create_task([&, i] {
                completion_index[i].store(counter.fetch_add(1));
                executed.fetch_add(1);
            }));
        }
        // Random-ish DAG: task i depends on a couple of earlier tasks.
        for (std::size_t i = 1; i < N; ++i) {
            dag.add_dependency(tasks[i / 2]->id, tasks[i]->id);
            if (i > 3) dag.add_dependency(tasks[i - 3]->id, tasks[i]->id);
        }
        dag.finalize();

        Runtime rt(4, policy);
        RunResult r = rt.run(dag);

        EXPECT_EQ(executed.load(), static_cast<int>(N)) << policy->name();
        EXPECT_EQ(r.records.size(), N) << policy->name();
        for (std::size_t i = 1; i < N; ++i) {
            EXPECT_LT(completion_index[i / 2].load(), completion_index[i].load())
                << "dep order violated for " << i << " under " << policy->name();
            if (i > 3) {
                EXPECT_LT(completion_index[i - 3].load(), completion_index[i].load())
                    << "dep order violated for " << i << " under " << policy->name();
            }
        }
    }
}

TEST(RuntimeTest, AllTasksCompleteOnStandardWorkloads) {
    auto pool = std::make_shared<MemoryPool>(16, 32 * 1024);
    for (const auto& w : standard_workloads()) {
        DAG dag = w.build(7, pool);
        const std::size_t expected = dag.size();
        for (auto& policy : all_policies()) {
            Runtime rt(4, policy);
            RunResult r = rt.run(dag);
            EXPECT_EQ(r.records.size(), expected)
                << w.name << " / " << policy->name();
            EXPECT_GT(r.makespan_ns, 0u) << w.name << " / " << policy->name();
        }
    }
}

TEST(RuntimeTest, SingleWorkerStillRespectsOrder) {
    DAG dag = build_chain(50, 100);
    for (auto& policy : all_policies()) {
        std::vector<std::atomic<int>> seen(50);
        for (auto& s : seen) s.store(0);
        Runtime rt(1, policy);
        RunResult r = rt.run(dag);
        EXPECT_EQ(r.records.size(), 50u) << policy->name();
    }
}

TEST(RuntimeTest, RunResultStatsAreSane) {
    DAG dag = build_layered(10, 50, 2000, 3);
    Runtime rt(4, make_critical_path_policy());
    RunResult r = rt.run(dag);
    EXPECT_GT(r.throughput(), 0.0);
    EXPECT_GT(r.utilization(), 0.0);
    EXPECT_LE(r.utilization(), 1.05); // allow small measurement noise
    EXPECT_LE(r.p50_latency(), r.p99_latency());
}

// Every scheduler must leave its ready queues empty when the run finishes.
TEST(RuntimeTest, PolicyQueueIsEmptyAfterRun) {
    for (auto& policy : all_policies()) {
        DAG dag = build_random(500, 2, 1000, 5);
        Runtime rt(4, policy);
        RunResult r = rt.run(dag);
        EXPECT_EQ(r.records.size(), dag.size()) << policy->name();
        EXPECT_EQ(policy->size(), 0u) << policy->name();
    }
}

// The locality-aware policy's whole point: tasks that share a data region should
// tend to run on the same core, so the region stays resident in that core's
// cache.  Compared against critical-path scheduling (which, for a source-only
// graph, dumps every task onto one worker and then scatters them via stealing).
TEST(LocalityTest, PreservesRegionColocation) {
    auto pool = std::make_shared<MemoryPool>(64, 64 * 1024);

    auto mean_workers_per_region = [&](const std::shared_ptr<SchedulingPolicy>& policy) {
        DAG dag = build_contended(48, 128, pool);
        Runtime rt(4, policy);
        RunResult r = rt.run(dag);
        std::map<DataId, std::set<WorkerId>> workers;
        for (const auto& rec : r.records) {
            const TaskHandle& t = dag.tasks()[rec.id];
            workers[t->data_accesses.front().id].insert(rec.worker);
        }
        double sum = 0.0;
        for (const auto& [region, ws] : workers) sum += static_cast<double>(ws.size());
        return sum / static_cast<double>(workers.size());
    };

    const double loc = mean_workers_per_region(make_locality_aware_policy());
    const double cp = mean_workers_per_region(make_critical_path_policy());

    // Ideal is 1.0 (a region lives on exactly one core); allow a little slack
    // for the odd steal.  The headline is that it beats critical-path by a lot.
    EXPECT_LT(loc, 2.5) << "locality-aware mean workers/region = " << loc;
    EXPECT_LT(loc, cp) << "locality-aware (" << loc << ") should beat critical-path ("
                       << cp << ") on region colocation";
}

// A source-only frontier is published before any worker runs, so queue depth is
// not yet a real load signal.  The adaptive policy must honor the static
// locality partition there rather than migrating the whole frontier away.
TEST(LocalityTest, DoesNotThrashTheSourceFrontier) {
    auto pool = std::make_shared<MemoryPool>(16, 32 * 1024);
    DAG dag = build_contended(24, 64, pool);
    Runtime rt(4, make_locality_aware_policy());
    RunResult r = rt.run(dag);
    EXPECT_EQ(r.records.size(), dag.size());
    EXPECT_EQ(r.conflict_migrations, 0u);
}
