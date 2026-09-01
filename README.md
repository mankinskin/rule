# rule

The rule system: canonical instruction/rule manifest model, storage, and
schema, exposed as a `rule` CLI and a `rule-mcp` server. Primary use case:
read and query the durable rule/instruction corpus that agent guidance is
built from, instead of hand-parsing manifest files.

## Quickstart

```bash
cargo build -p rule --bin rule --features cli
./target/debug/rule --help
```

The `rule-mcp` binary (feature `mcp`) exposes the same operations as MCP
tools; see [crates/rule-api](crates/rule-api) for the underlying model.
