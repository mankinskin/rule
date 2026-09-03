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

use std::{
    fs,
    path::{
        Path,
        PathBuf,
    },
    process::Command,
};

use chrono::Utc;
use criterion::{
    BatchSize,
    Criterion,
    criterion_group,
    criterion_main,
};
use memory_kernel::{
    model::edge::EdgeRecord,
    storage::move_kernel::{
        MoveBlocker,
        MoveExecutionPhase,
        MovePlan,
    },
};
use rule_api::{
    manifest::RuleManifest,
    store::RuleStore,
};
use tempfile::TempDir;
use uuid::Uuid;

fn git_init(repo_root: &Path) {
    let status = Command::new("git")
        .current_dir(repo_root)
        .arg("init")
        .status()
        .expect("run git init");
    assert!(status.success(), "git init failed");
}

/// Link topology between the moved rule(s) and the rest of the fixture.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LinkTopology {
    /// No edges at all.
    None,
    /// Edges from each moved rule to other rules in the same moved-candidate
    /// pool (never a separate external pool).
    Internal,
    /// Edges from each moved rule to a fixed, separate external pool that
    /// never participates as a moved candidate.
    Crossing,
}

const CROSSING_EXTERNAL_POOL: usize = 20;

/// One isolated source+target workspace pair with `moved_count` moved rules,
/// `background_count` unrelated rules already in the source store (used to
/// vary total store size independent of the moved batch), and `density`
/// edges per moved rule under the given `topology`.
fn build_rule_fixture(
    moved_count: usize,
    topology: LinkTopology,
    density: usize,
    background_count: usize,
) -> (TempDir, RuleStore, PathBuf, Vec<Uuid>) {
    let workspace_dir = tempfile::tempdir().expect("tempdir");
    let repo = workspace_dir.path().join("repo");
    fs::create_dir_all(&repo).expect("create repo dir");
    git_init(&repo);

    let source_workspace = repo.join("source");
    let target_workspace = repo.join("target");
    fs::create_dir_all(&source_workspace).expect("create source workspace");
    fs::create_dir_all(&target_workspace).expect("create target workspace");

    let mut source_store =
        RuleStore::init(&source_workspace).expect("init source store");
    RuleStore::init(&target_workspace).expect("init target store");

    let moved_ids: Vec<Uuid> = (0..moved_count)
        .map(|offset| {
            let manifest = RuleManifest::new(
                &format!("bench/moved-{offset}"),
                &format!("Moved rule {offset}"),
                "agents",
                "main",
                "moved rule body",
            );
            source_store
                .create(&manifest, None)
                .expect("create moved rule")
        })
        .collect();

    let external_ids: Vec<Uuid> =
        if topology == LinkTopology::Crossing && density > 0 {
            (0..CROSSING_EXTERNAL_POOL)
                .map(|offset| {
                    let manifest = RuleManifest::new(
                        &format!("bench/external-{offset}"),
                        &format!("External rule {offset}"),
                        "agents",
                        "main",
                        "external rule body",
                    );
                    source_store
                        .create(&manifest, None)
                        .expect("create external rule")
                })
                .collect()
        } else {
            Vec::new()
        };

    for offset in 0..background_count {
        let manifest = RuleManifest::new(
            &format!("bench/background-{offset}"),
            &format!("Background rule {offset}"),
            "agents",
            "main",
            "background rule body",
        );
        source_store
            .create(&manifest, None)
            .expect("create background rule");
    }

    if density > 0 {
        let now = Utc::now();
        match topology {
            LinkTopology::None => {},
            LinkTopology::Crossing => {
                let crossing_density = density.min(external_ids.len());
                for (idx, moved_id) in moved_ids.iter().enumerate() {
                    for step in 0..crossing_density {
                        let target_idx = (idx + step) % external_ids.len();
                        source_store
                            .entity_store()
                            .add_edge(EdgeRecord {
                                from: *moved_id,
                                to: external_ids[target_idx],
                                kind: "linked".to_string(),
                                created_at: now,
                            })
                            .expect("add crossing edge");
                    }
                }
            },
            LinkTopology::Internal => {
                // Distinct partners available per moved rule, excluding itself.
                let available_partners = moved_ids.len().saturating_sub(1);
                let internal_density = density.min(available_partners);
                for (idx, moved_id) in moved_ids.iter().enumerate() {
                    for step in 0..internal_density {
                        // Skip self by offsetting by one before wrapping.
                        let target_idx =
                            (idx + step + 1) % moved_ids.len();
                        source_store
                            .entity_store()
                            .add_edge(EdgeRecord {
                                from: *moved_id,
                                to: moved_ids[target_idx],
                                kind: "linked".to_string(),
                                created_at: now,
                            })
                            .expect("add internal edge");
                    }
                }
            },
        }
    }

    source_store.scan(true).expect("scan source store");

    (workspace_dir, source_store, target_workspace, moved_ids)
}

/// Build a supported preflight plan for `id`, dropping the blockers that are
/// expected artifacts of the isolated bench fixture rather than genuine
/// domain conflicts (mirrors the existing rule move unit tests).
fn active_move_plan(
    store: &RuleStore,
    target_root: &Path,
    id: &Uuid,
) -> MovePlan {
    let mut plan = store
        .plan_move_preflight(id, target_root)
        .expect("plan preflight");
    plan.blockers.retain(|blocker| {
        !matches!(
            blocker,
            MoveBlocker::PathReferenceScanUnavailable { .. }
                | MoveBlocker::DirtyTrackedFiles { .. }
        )
    });
    assert!(plan.supported(), "unexpected move blockers: {:?}", plan.blockers);
    plan
}

// --- Entity count ---

fn bench_rule_move_preflight_by_entity_count(c: &mut Criterion) {
    for &moved_count in &[1usize, 25, 100, 500] {
        let (_workspace_dir, store, target_root, ids) =
            build_rule_fixture(moved_count, LinkTopology::None, 0, 0);
        let id = ids[0];
        c.bench_function(
            &format!("rule_move_preflight_{moved_count}entities"),
            |b| {
                b.iter(|| {
                    let plan = store
                        .plan_move_preflight(&id, &target_root)
                        .expect("plan preflight");
                    criterion::black_box(plan);
                });
            },
        );
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
            let (_workspace_dir, store, target_root, ids) =
                build_rule_fixture(MOVED_COUNT, topology, density, 0);
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
    c.bench_function("rule_move_phase_preflight_only", |b| {
        b.iter_batched(
            || build_rule_fixture(1, LinkTopology::None, 0, 0),
            |(_workspace_dir, store, target_root, ids)| {
                let plan = store
                    .plan_move_preflight(&ids[0], &target_root)
                    .expect("plan preflight");
                criterion::black_box(plan);
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_rule_move_apply_only(c: &mut Criterion) {
    c.bench_function("rule_move_phase_apply_only", |b| {
        b.iter_batched(
            || {
                let (workspace_dir, store, target_root, ids) =
                    build_rule_fixture(1, LinkTopology::None, 0, 0);
                let plan = active_move_plan(&store, &target_root, &ids[0]);
                (workspace_dir, store, plan)
            },
            |(_workspace_dir, store, plan)| {
                let outcome = store
                    .execute_move_with_journal(&plan)
                    .expect("execute move");
                assert_eq!(outcome.journal.phase, MoveExecutionPhase::Validated);
                criterion::black_box(outcome);
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_rule_move_preflight_plus_apply(c: &mut Criterion) {
    c.bench_function("rule_move_phase_preflight_plus_apply", |b| {
        b.iter_batched(
            || build_rule_fixture(1, LinkTopology::None, 0, 0),
            |(_workspace_dir, store, target_root, ids)| {
                let plan = active_move_plan(&store, &target_root, &ids[0]);
                let outcome = store
                    .execute_move_with_journal(&plan)
                    .expect("execute move");
                assert_eq!(outcome.journal.phase, MoveExecutionPhase::Validated);
                criterion::black_box(outcome);
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_rule_move_rollback(c: &mut Criterion) {
    c.bench_function("rule_move_phase_rollback", |b| {
        b.iter_batched(
            || {
                let (workspace_dir, store, target_root, ids) =
                    build_rule_fixture(1, LinkTopology::None, 0, 0);
                let plan = active_move_plan(&store, &target_root, &ids[0]);
                let outcome = store
                    .execute_move_with_journal(&plan)
                    .expect("execute move");
                (workspace_dir, store, outcome.journal.id)
            },
            |(_workspace_dir, store, journal_id)| {
                let outcome = store
                    .rollback_move_with_journal(journal_id)
                    .expect("rollback move");
                assert!(outcome.rolled_back);
                criterion::black_box(outcome);
            },
            BatchSize::SmallInput,
        );
    });
}

/// Coverage limitation: this benchmarks `resume_move_with_journal` called on
/// an already-`Validated` journal (an idempotent re-resume), since the
/// public move API cannot synthesize a genuinely-interrupted move. See the
/// module doc comment.
fn bench_rule_move_resume_idempotent_proxy(c: &mut Criterion) {
    c.bench_function("rule_move_phase_resume_idempotent_proxy", |b| {
        b.iter_batched(
            || {
                let (workspace_dir, store, target_root, ids) =
                    build_rule_fixture(1, LinkTopology::None, 0, 0);
                let plan = active_move_plan(&store, &target_root, &ids[0]);
                let outcome = store
                    .execute_move_with_journal(&plan)
                    .expect("execute move");
                (workspace_dir, store, outcome.journal.id)
            },
            |(_workspace_dir, store, journal_id)| {
                let outcome = store
                    .resume_move_with_journal(journal_id)
                    .expect("resume move");
                criterion::black_box(outcome);
            },
            BatchSize::SmallInput,
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
        c.bench_function(
            &format!("rule_move_apply_store_size_{total_store_size}rules"),
            |b| {
                b.iter_batched(
                    || {
                        let (workspace_dir, store, target_root, ids) =
                            build_rule_fixture(
                                MOVED_COUNT,
                                LinkTopology::Crossing,
                                DENSITY,
                                background_count,
                            );
                        let plan =
                            active_move_plan(&store, &target_root, &ids[0]);
                        (workspace_dir, store, plan)
                    },
                    |(_workspace_dir, store, plan)| {
                        let outcome = store
                            .execute_move_with_journal(&plan)
                            .expect("execute move");
                        assert_eq!(
                            outcome.journal.phase,
                            MoveExecutionPhase::Validated
                        );
                        criterion::black_box(outcome);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

criterion_group!(
    move_health,
    bench_rule_move_preflight_by_entity_count,
    bench_rule_move_preflight_by_link_density,
    bench_rule_move_preflight_only,
    bench_rule_move_apply_only,
    bench_rule_move_preflight_plus_apply,
    bench_rule_move_rollback,
    bench_rule_move_resume_idempotent_proxy,
    bench_rule_move_apply_by_store_size,
);
criterion_main!(move_health);
