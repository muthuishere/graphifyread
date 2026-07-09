# Plan — graphifyread in Go, S3-backed graph, config-driven

Status: proposal · Date: 2026-07-09 · Owner: @muthuishere

## 1. What we're building (and what we're not)

Build a **Go read engine** for the graphify knowledge graph. The graph is no
longer a per-project `graphify-out/graph.json` on someone's laptop — it is a
**single canonical graph** kept in an **S3 bucket** (or, for small/OSS repos, a
single JSON committed to the monorepo). A **`graphify.config.json`** in each repo
tells the Go binary where that graph lives. Skills for every AI coding agent wrap
the Go binary so `/graphify query …` answers from the central graph.

Scope is deliberately the **read half** of the pipeline (the repo is named
`graphifyread`):

```
Python owns (unchanged):   detect → extract → build → cluster → analyze → [writes graph.json]
Go owns (this plan):       load ← S3/local ─ query · path · explain · report · serve(MCP) · export
```

This split works because **read is language-agnostic**: every read command in the
current Python code (`query`, `path`, `explain`, `serve`) consumes only
`graph.json` (nodes + edges + `community`). None of it needs tree-sitter or an
LLM. So the 735 KB `extract.py` and the dozens of language parsers stay in Python;
Go re-implements pure graph traversal, which is small, fast, and easy to make
byte-for-byte conformant.

**Non-goals for v0.1:** porting extraction/AST/build/cluster, HTML/SVG/Obsidian
viz, video transcription, semantic (LLM) extraction. These stay in Python and are
candidate follow-ons (§9).

## 2. The two things that change vs. today

| Today (Python) | This plan (Go) |
|---|---|
| Graph is local `graphify-out/graph.json` per project | Graph is **central**: one S3 object (or one committed monorepo JSON) shared by every repo/agent |
| Interpreter bootstrap in `skill.md` (`uv tool install graphifyy`, `.graphify_python`) | Single static Go binary `graphifyread`, no Python/venv/interpreter dance |
| Location is implicit (cwd `graphify-out/`) | Location is explicit: **`graphify.config.json`** → backend + bucket + key |

Everything a read command does today it will still do — it just resolves the graph
through a **store seam** instead of `open("graphify-out/graph.json")`.

## 3. Repository layout (mirror the citenexus polyglot pattern)

The sibling repo `citenexus` already ships a Go port as a monorepo submodule
(`module github.com/muthuishere/citenexus/golang`, tagged `golang/vX.Y.Z`,
deterministic `fakes/`, shared `conformance/`). Reuse that shape verbatim:

```
graphifyread/
  golang/
    go.mod                       # module github.com/muthuishere/graphifyread/golang  (go 1.26)
    cmd/graphifyread/main.go     # CLI dispatch
    config/                      # load + validate graphify.config.json, env overrides
    store/                       # the seam
      store.go                   #   GraphStore interface (Load/Save/Stat)
      local.go                   #   file backend  (monorepo JSON, graphify-out/)
      s3.go                      #   S3 backend    (aws-sdk-go-v2; MinIO-compatible)
    graph/                       # in-memory model: Node, Edge, Graph, community index, adjacency
    query/                       # BFS/DFS traversal, budget cap, vocab expansion
    path/                        # shortest path between two concepts
    explain/                     # single-node neighborhood explanation
    report/                      # render GRAPH_REPORT.md from a loaded graph
    serve/                       # MCP stdio server (read tools)
    fakes/                       # in-memory GraphStore for hermetic tests
    internal/conform/            # conformance harness
  skills/                        # per-agent skill.md that shell out to the binary (generated)
  conformance/                   # shared fixtures: graph.json + expected query/path/explain outputs
  docs/plans/                    # this file
```

`golang/` is self-contained (own `go.mod`, own tests) but versioned with the repo,
exactly like `citenexus/golang`.

## 4. The store seam — `graphify.config.json`

One interface, two backends. This is the heart of the ask ("single monorepo json
**or** a config.json which connects from s3 bucket").

```go
// store/store.go
type GraphStore interface {
    Load(ctx context.Context) (*graph.Graph, error) // pull graph.json → model
    Save(ctx context.Context, g *graph.Graph) error // push model → graph.json (build side / later)
    Stat(ctx context.Context) (Meta, error)         // etag/size/mtime for cache & staleness
}
```

- **`local.go`** — reads a JSON file. Covers *both* "single monorepo JSON checked
  into the repo" and the legacy `graphify-out/graph.json`.
- **`s3.go`** — `aws-sdk-go-v2`, endpoint-overridable so the **MinIO** setup already
  used in `citenexus` (`compose.yaml`, `:19000`) works for local integration tests.
  Optional local cache keyed by ETag so repeated reads don't re-download.

### config schema (v1)

```jsonc
{
  "version": 1,
  "graph_id": "acme-monorepo",          // logical name; also the default S3 key stem
  "store": {
    "backend": "s3",                    // "s3" | "local"
    "s3": {
      "bucket": "acme-graphs",
      "prefix": "graphify",             // object = <prefix>/<graph_id>/graph.json
      "region": "us-east-1",
      "endpoint": "",                   // set for MinIO / R2 / non-AWS
      "cache": true
    },
    "local": { "path": "graphify-out/graph.json" }
  },
  "read": { "traversal": "bfs", "default_budget": 1500 }
}
```

Resolution order: explicit `--config` flag → `./graphify.config.json` → `$GRAPHIFYREAD_CONFIG`
→ fall back to local `graphify-out/graph.json` (zero-config parity with today).
Credentials come **only** from the standard AWS chain (`AWS_*`, profile, IRSA) —
never stored in `config.json`, never logged (carry over the repo's secret-hygiene rule).

## 5. What to port, module by module

All from the current Python read path (`__main__.py`, `references/query.md`,
`serve.py`). Pure graph ops — no external deps beyond the JSON + AWS SDK.

| Go pkg | Ports | Behavior to preserve exactly |
|---|---|---|
| `graph` | `build.py` load half, `export.to_json` shape | node/edge/`community` fields; `directed`/`multigraph` flags; id parity |
| `query` | `query` BFS/DFS | vocab expansion vs. graph terms, `--budget` token cap, quote `source_location` when citing, "answer only from graph" |
| `path` | `path` | shortest path source→target, report crossed community boundaries |
| `explain` | `explain` | node + neighbors + community, plain-language summary payload |
| `report` | `report.generate` | GRAPH_REPORT.md: god nodes, surprising connections, suggested questions, **raw cohesion numbers**, token cost — honor the Honesty Rules |
| `serve` | `serve.py` (read tools only) | MCP stdio: `query`, `path`, `explain`, `get_node`, `neighbors`, `subgraph` |

Budget/token counting: match the Python tokenizer's *counting semantics* (used only
for the `--budget` cap and report cost line), not a specific library — pin it with a
conformance fixture so Go and Python agree on a fixed graph.

## 6. CLI surface (v0.1)

```
graphifyread query "<question>" [--dfs] [--budget N] [--config path]
graphifyread path  "<A>" "<B>"
graphifyread explain "<Node>"
graphifyread report                       # print/refresh GRAPH_REPORT.md from the store
graphifyread serve                        # MCP stdio server (read tools)
graphifyread config init|show|validate    # scaffold & check graphify.config.json
graphifyread pull                         # S3 → local cache (prewarm / offline)
```

`query`/`path`/`explain`/`serve` mirror the current `/graphify` subcommands so the
skills change *how* they call, not *what* users type.

## 7. Skills — install the Go binary, not a Python env

Today `skill.md` bootstraps a Python interpreter (`uv tool install graphifyy`,
writes `.graphify_python`, re-exec's `python3` per step). Replace that whole dance
with: **ensure the `graphifyread` binary is on PATH; every step calls it.**

- Keep the existing multi-agent fan-out under `graphify/skills/<agent>/`
  (claude, codex, kilo, droid, trae, pi, amp, copilot, …) and the `tools/skillgen/`
  generator + golden `expected/` files — regenerate them for the Go CLI.
- New install path: download a prebuilt `graphifyread` binary from a GitHub release
  (goreleaser, per-OS/arch) **or** `go install github.com/muthuishere/graphifyread/golang/cmd/graphifyread@latest`.
- The "fast path — existing graph" logic in `skill.md` stays, but the existence
  check becomes `graphifyread config show` (does the store resolve?) instead of
  statting `graphify-out/graph.json`.
- Read-only agents (Explore-type) can now serve queries safely — no interpreter,
  no writes, just a binary hitting S3.

## 8. Milestones (test-first, red→green per step)

- **M0 — Scaffold.** `golang/go.mod`, `cmd/graphifyread`, CI (`go test ./...`, vet,
  staticcheck), `conformance/` with one real `graph.json` from `worked/`. Wire the
  citenexus-style `internal/conform` harness.
- **M1 — Model + local store.** `graph` loads/round-trips existing `graph.json`
  byte-shape; `store/local.go`; `config` load/validate; `config init|show`.
  Conformance: load every `worked/*/graphify-out/graph.json`.
- **M2 — Read commands.** `query` (BFS+DFS+budget), `path`, `explain`. Each ships
  with conformance fixtures diffed against Python output on frozen graphs.
- **M3 — S3 store.** `store/s3.go` + MinIO integration test (reuse citenexus
  `compose.yaml` pattern, high ports). `pull`, ETag cache. `report` from the store.
- **M4 — MCP serve.** `serve` stdio server, read tools, one end-to-end test with a
  fake MCP client over a fixture graph.
- **M5 — Skills + release.** Regenerate `skills/<agent>/` for the binary;
  goreleaser cross-compile; tag `golang/v0.1.0`. Ship.

## 9. Deferred / follow-on

- Port the **build side** to Go (detect/AST via `go-tree-sitter`, cluster via a Go
  Leiden/Louvain) so Go can `Save()` too and drop the Python dependency entirely.
- `export` to HTML/SVG/Obsidian/Neo4j/FalkorDB.
- `--watch` incremental rebuild; multi-repo cross-graph merge into one S3 graph.
- Graph versioning in S3 (object versions / `graph_id@rev`) + shrink-guard (§479 in
  Python) enforced on `Save()`.

## 10. Open decisions (resolve before M0)

1. **Scope confirm:** read-only Go v0.1 (recommended), or also port build now?
2. **S3 object convention:** one `graph.json` per `graph_id`, or versioned keys +
   a `latest` pointer? (Recommend versioned + `latest` for safe concurrent writes.)
3. **Distribution:** prebuilt release binary (recommended for the skill install) vs.
   `go install`-only.
4. **Dist/module name:** confirm `github.com/muthuishere/graphifyread/golang`
   (matches the citenexus tag convention `golang/vX.Y.Z`).
5. **Conformance source of truth:** freeze a handful of `worked/` graphs as the
   cross-language contract (like `citenexus/conformance/`)?
```
