//! Move-health Criterion benchmarks for [`rule_api::move_domain::RuleMoveDomain`].
//!
//! Coverage: entity count, link topology (none / internal moved-to-moved /
//! crossing moved-to-external), link density, phase separation
//! (preflight_only / apply_only / preflight_plus_apply / rollback / resume),
//! and a fixed-total store-size comparison. The store-size scenario exists
//! because `RuleMoveDomain` does not override
//! `MoveDomain::reconcile_store_touched`, so execution falls back to the
//! kernel's default `scan_store` reconciliation (`RuleStore::scan(true)`),
//! which rescans the *entire* destination store rather than the touched
//! entity alone. Apply cost is therefore expected to scale with total store
//! size, not just the moved entity (see `IMPLEMENTATION-REVIEW.md` F5).
//!
//! Coverage limitation: resume is benchmarked as an idempotent re-resume of
//! an already-`Validated` journal. The public API (`plan_move_preflight` /
//! `execute_move_with_journal` / `resume_move_with_journal` /
//! `rollback_move_with_journal`) has no entry point that interrupts execution
//! mid-phase (`execute_or_resume` runs every phase to completion in one
//! call), so a genuinely-interrupted resume (from a persisted `Locked`-phase
//! journal) cannot be constructed without duplicating kernel-internal
//! execution logic in the benchmark. This is recorded in the benchmark name
//! (`..._resume_idempotent_proxy`).
//!
//! Internal-link topology note: the move kernel moves one entity per call,
//! so "internal" here means an edge between the benchmarked moved entity and
//! another entity that is *also* present in the moved-candidate pool for
//! this fixture (but not itself moved in this particular call) — distinct
//! from "crossing", where the edge points at a separate external pool that
//! never participates as a moved candidate.

use std::path::{Path, PathBuf};

use chrono::Utc;
use criterion::{Criterion, criterion_group, criterion_main};
use memory_kernel::{
    model::edge::EdgeRecord,
    storage::move_kernel::{MoveExecutionPhase, MovePlan},
    testing::{
        MoveBenchmarkWorkspace, drop_fixture_blockers, iter_move_benchmark, move_bench_criterion,
    },
};
use rule_api::{manifest::RuleManifest, store::RuleStore};
use uuid::Uuid;

mod move_health_fixture;
use move_health_fixture::{LinkTopology, build_rule_fixture};

/// Build a supported preflight plan for `id`, dropping the blockers that are
/// expected artifacts of the isolated bench fixture rather than genuine
/// domain conflicts (mirrors the existing rule move unit tests).
fn active_move_plan(store: &RuleStore, target_root: &Path, id: &Uuid) -> MovePlan {
    let mut plan = store
        .plan_move_preflight(id, target_root)
        .expect("plan preflight");
    drop_fixture_blockers(&mut plan);
    assert!(
        plan.supported(),
        "unexpected move blockers: {:?}",
        plan.blockers
    );
    plan
}

// --- Entity count ---

fn bench_rule_move_preflight_by_entity_count(c: &mut Criterion) {
    for &moved_count in &[1usize, 25, 100, 500] {
        let workspace = MoveBenchmarkWorkspace::new();
        let (store, target_root, ids) =
            build_rule_fixture(&workspace, moved_count, LinkTopology::None, 0, 0);
        let id = ids[0];
        c.bench_function(&format!("rule_move_preflight_{moved_count}entities"), |b| {
            b.iter(|| {
                let plan = store
                    .plan_move_preflight(&id, &target_root)
                    .expect("plan preflight");
                criterion::black_box(plan);
            });
        });
    }
}

// --- Link topology and density ---

fn bench_rule_move_preflight_by_link_density(c: &mut Criterion) {
    const MOVED_COUNT: usize = 25;
    for &(topology, label) in &[
        (LinkTopology::Internal, "internal"),
        (LinkTopology::Crossing, "crossing"),
    ] {
        for &density in &[1usize, 5, 20] {
            let workspace = MoveBenchmarkWorkspace::new();
            let (store, target_root, ids) =
                build_rule_fixture(&workspace, MOVED_COUNT, topology, density, 0);
            let id = ids[0];
            c.bench_function(
                &format!("rule_move_preflight_{label}_{density}links"),
                |b| {
                    b.iter(|| {
                        let plan = store
                            .plan_move_preflight(&id, &target_root)
                            .expect("plan preflight");
                        criterion::black_box(plan.reference_visibility.len());
                    });
                },
            );
        }
    }
}

// --- Phase separation ---

fn bench_rule_move_preflight_only(c: &mut Criterion) {
    let workspace = MoveBenchmarkWorkspace::new();
    let (store, target_root, ids) = build_rule_fixture(&workspace, 1, LinkTopology::None, 0, 0);
    let id = ids[0];
    c.bench_function("rule_move_phase_preflight_only", |b| {
        b.iter(|| {
            let plan = store
                .plan_move_preflight(&id, &target_root)
                .expect("plan preflight");
            criterion::black_box(plan);
        });
    });
}

fn bench_rule_move_apply_only(c: &mut Criterion) {
    let workspace = MoveBenchmarkWorkspace::new();
    c.bench_function("rule_move_phase_apply_only", |b| {
        iter_move_benchmark(
            b,
            || {
                let (store, target_root, ids) =
                    build_rule_fixture(&workspace, 1, LinkTopology::None, 0, 0);
                let plan = active_move_plan(&store, &target_root, &ids[0]);
                (store, plan)
            },
            |(store, plan)| {
                let outcome = store
                    .execute_move_with_journal(&plan)
                    .expect("execute move");
                assert_eq!(outcome.journal.phase, MoveExecutionPhase::Validated);
                criterion::black_box(outcome);
            },
        );
    });
}

fn bench_rule_move_set_preflight(c: &mut Criterion) {
    let workspace = MoveBenchmarkWorkspace::new();
    c.bench_function("rule_move_set_preflight_2entities", |b| {
        iter_move_benchmark(
            b,
            || {
                let (store, target_root, ids) =
                    build_rule_fixture(&workspace, 1, LinkTopology::None, 0, 0);
                (store, target_root, ids)
            },
            |(store, target_root, ids)| {
                let plan = store
                    .plan_move_set(&ids, &target_root)
                    .expect("plan move set");
                criterion::black_box(plan);
            },
        );
    });
}

fn bench_rule_move_preflight_plus_apply(c: &mut Criterion) {
    let workspace = MoveBenchmarkWorkspace::new();
    c.bench_function("rule_move_phase_preflight_plus_apply", |b| {
        iter_move_benchmark(
            b,
            || build_rule_fixture(&workspace, 1, LinkTopology::None, 0, 0),
            |(store, target_root, ids)| {
                let plan = active_move_plan(&store, &target_root, &ids[0]);
                let outcome = store
                    .execute_move_with_journal(&plan)
                    .expect("execute move");
                assert_eq!(outcome.journal.phase, MoveExecutionPhase::Validated);
                criterion::black_box(outcome);
            },
        );
    });
}

fn bench_rule_move_rollback(c: &mut Criterion) {
    let workspace = MoveBenchmarkWorkspace::new();
    c.bench_function("rule_move_phase_rollback", |b| {
        iter_move_benchmark(
            b,
            || {
                let (store, target_root, ids) =
                    build_rule_fixture(&workspace, 1, LinkTopology::None, 0, 0);
                let plan = active_move_plan(&store, &target_root, &ids[0]);
                let outcome = store
                    .execute_move_with_journal(&plan)
                    .expect("execute move");
                (store, outcome.journal.id)
            },
            |(store, journal_id)| {
                let outcome = store
                    .rollback_move_with_journal(journal_id)
                    .expect("rollback move");
                assert!(outcome.rolled_back);
                criterion::black_box(outcome);
            },
        );
    });
}

/// Coverage limitation: this benchmarks `resume_move_with_journal` called on
/// an already-`Validated` journal (an idempotent re-resume), since the
/// public move API cannot synthesize a genuinely-interrupted move. See the
/// module doc comment.
fn bench_rule_move_resume_idempotent_proxy(c: &mut Criterion) {
    let workspace = MoveBenchmarkWorkspace::new();
    c.bench_function("rule_move_phase_resume_idempotent_proxy", |b| {
        iter_move_benchmark(
            b,
            || {
                let (store, target_root, ids) =
                    build_rule_fixture(&workspace, 1, LinkTopology::None, 0, 0);
                let plan = active_move_plan(&store, &target_root, &ids[0]);
                let outcome = store
                    .execute_move_with_journal(&plan)
                    .expect("execute move");
                (store, outcome.journal.id)
            },
            |(store, journal_id)| {
                let outcome = store
                    .resume_move_with_journal(journal_id)
                    .expect("resume move");
                criterion::black_box(outcome);
            },
        );
    });
}

// --- Fixed total store-size comparison ---
//
// `RuleMoveDomain` has no `reconcile_store_touched` override, so execution
// falls back to the kernel default, which rescans the destination store in
// full (`RuleStore::scan(true)`). Apply cost is therefore expected to scale
// with total store size, not just the touched entity (F5).

fn bench_rule_move_apply_by_store_size(c: &mut Criterion) {
    const MOVED_COUNT: usize = 5;
    const DENSITY: usize = 5;
    for &background_count in &[10usize, 100, 400] {
        let total_store_size = MOVED_COUNT + background_count;
        let workspace = MoveBenchmarkWorkspace::new();
        c.bench_function(
            &format!("rule_move_apply_store_size_{total_store_size}rules"),
            |b| {
                iter_move_benchmark(
                    b,
                    || {
                        let (store, target_root, ids) = build_rule_fixture(
                            &workspace,
                            MOVED_COUNT,
                            LinkTopology::Crossing,
                            DENSITY,
                            background_count,
                        );
                        let plan = active_move_plan(&store, &target_root, &ids[0]);
                        (store, plan)
                    },
                    |(store, plan)| {
                        let outcome = store
                            .execute_move_with_journal(&plan)
                            .expect("execute move");
                        assert_eq!(outcome.journal.phase, MoveExecutionPhase::Validated);
                        criterion::black_box(outcome);
                    },
                );
            },
        );
    }
}

fn criterion_config() -> Criterion {
    move_bench_criterion()
}

criterion_group!(
    name = move_health;
    config = criterion_config();
    targets =
    bench_rule_move_preflight_by_entity_count,
    bench_rule_move_preflight_by_link_density,
    bench_rule_move_preflight_only,
    bench_rule_move_apply_only,
    bench_rule_move_preflight_plus_apply,
    bench_rule_move_rollback,
    bench_rule_move_resume_idempotent_proxy,
    bench_rule_move_apply_by_store_size,
    bench_rule_move_set_preflight
);
criterion_main!(move_health);
