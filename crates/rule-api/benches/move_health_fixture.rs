use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LinkTopology {
    None,
    Internal,
    Crossing,
}

const CROSSING_EXTERNAL_POOL: usize = 20;

fn create_rules(store: &mut RuleStore, count: usize, kind: &str) -> Vec<Uuid> {
    (0..count)
        .map(|offset| {
            let manifest = RuleManifest::new(
                &format!("bench/{kind}-{offset}"),
                &format!("{kind} rule {offset}"),
                "agents",
                "main",
                &format!("{kind} rule body"),
            );
            store.create(&manifest, None).expect("create fixture rule")
        })
        .collect()
}

fn create_edges(
    store: &RuleStore,
    topology: LinkTopology,
    density: usize,
    moved_ids: &[Uuid],
    external_ids: &[Uuid],
) {
    let now = Utc::now();
    match topology {
        LinkTopology::None => {}
        LinkTopology::Crossing => {
            let density = density.min(external_ids.len());
            for (index, moved_id) in moved_ids.iter().enumerate() {
                for step in 0..density {
                    store
                        .entity_store()
                        .add_edge(EdgeRecord {
                            from: *moved_id,
                            to: external_ids[(index + step) % external_ids.len()],
                            kind: "linked".to_string(),
                            created_at: now,
                        })
                        .expect("add crossing edge");
                }
            }
        }
        LinkTopology::Internal => {
            let density = density.min(moved_ids.len().saturating_sub(1));
            for (index, moved_id) in moved_ids.iter().enumerate() {
                for step in 0..density {
                    store
                        .entity_store()
                        .add_edge(EdgeRecord {
                            from: *moved_id,
                            to: moved_ids[(index + step + 1) % moved_ids.len()],
                            kind: "linked".to_string(),
                            created_at: now,
                        })
                        .expect("add internal edge");
                }
            }
        }
    }
}

pub fn build_rule_fixture(
    workspace: &MoveBenchmarkWorkspace,
    moved_count: usize,
    topology: LinkTopology,
    density: usize,
    background_count: usize,
) -> (RuleStore, PathBuf, Vec<Uuid>) {
    workspace.reset();
    let source_root = workspace.source_root();
    let target_root = workspace.target_root().to_path_buf();
    let mut source_store = RuleStore::init(source_root).expect("init source store");
    RuleStore::init(&target_root).expect("init target store");

    let moved_ids = create_rules(&mut source_store, moved_count, "moved");
    let external_ids = if topology == LinkTopology::Crossing && density > 0 {
        create_rules(&mut source_store, CROSSING_EXTERNAL_POOL, "external")
    } else {
        Vec::new()
    };
    create_rules(&mut source_store, background_count, "background");
    source_store.scan(true).expect("scan source store");
    create_edges(&source_store, topology, density, &moved_ids, &external_ids);
    (source_store, target_root, moved_ids)
}
