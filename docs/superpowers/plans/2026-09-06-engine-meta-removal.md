# Engine Metadata Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the engine's write-only payload metadata (`AlertMeta`, `AlertSpec.Meta`, `Engine.meta`) — ~160 MB of dead memory per 1M alerts.

**Architecture:** The engine evaluates alerts and carries no notification payload. Enrichment already happens store-side (`Core.deliverBatch` → `Store.BatchGet`). Removal is ordered so every commit is green: the service stops filling `Meta` first, then the engine deletes the type and map.

**Tech Stack:** Go 1.27, standard `go test`. Spec: `docs/superpowers/specs/2026-09-06-engine-meta-removal-design.md`.

## Global Constraints

- The `engine/` package freeze is lifted for exactly this change; no other engine behavior may change (conformance, oracle, and parity suites must stay green, unmodified — do not edit engine test expectations beyond the one `Meta:` fill listed).
- No wire (proto), alertstore, or UI changes.
- Each commit must leave the tree building and tests passing.

---

### Task 1: Service stops filling Meta

**Files:**
- Modify: `internal/service/core.go:216` (in `specFrom`) and `core.go:344` (in `UpsertAlert`)

**Interfaces:**
- Consumes: `engine.AlertSpec` (unchanged this task — `Meta` field still exists)
- Produces: `AlertSpec` literals with no `Meta` fill; nothing downstream reads it, so no other task depends on this shape

No new test: this is a removal with no observable behavior. The guard is the existing `internal/service` suite plus compile success.

- [ ] **Step 1: Delete the Meta fill in `specFrom`**

In `internal/service/core.go`, `specFrom` currently returns (around line 209-221):

```go
	return engine.AlertSpec{
		ID: a.ID, Symbol: sym.Name, PriceType: a.PriceType, Direction: a.Direction,
		TargetPrice:    a.TargetPrice,
		ValidFrom:      a.ValidFrom,
		Expires:        a.Expires,
		AutoDeactivate: a.AutoDeactivate,
		Dims:           engine.Dims(vv, tv),
		Meta: engine.AlertMeta{
			ID: a.ID, Symbol: sym.Name, PriceType: a.PriceType, Direction: a.Direction,
			TargetPrice: a.TargetPrice, CreatedAt: a.CreatedAt,
		},
	}, true
```

Delete the `Meta: engine.AlertMeta{…}` block, leaving:

```go
	return engine.AlertSpec{
		ID: a.ID, Symbol: sym.Name, PriceType: a.PriceType, Direction: a.Direction,
		TargetPrice:    a.TargetPrice,
		ValidFrom:      a.ValidFrom,
		Expires:        a.Expires,
		AutoDeactivate: a.AutoDeactivate,
		Dims:           engine.Dims(vv, tv),
	}, true
```

- [ ] **Step 2: Delete the Meta fill in `UpsertAlert`**

In `Core.UpsertAlert` (around line 337-348), the literal currently is:

```go
	spec := engine.AlertSpec{
		ID: id, Symbol: sym.Name, PriceType: pt, Direction: dir,
		TargetPrice:    engine.Price(base),
		ValidFrom:      req.GetValidFromUnixNanos(),
		Expires:        req.GetExpiresUnixNanos(),
		AutoDeactivate: req.GetAutoDeactivate(),
		Dims:           engine.Dims(vv, tv),
		Meta: engine.AlertMeta{
			ID: id, Symbol: sym.Name, PriceType: pt, Direction: dir,
			TargetPrice: engine.Price(base), CreatedAt: rec.CreatedAt,
		},
	}
```

Delete the `Meta: engine.AlertMeta{…}` block, leaving:

```go
	spec := engine.AlertSpec{
		ID: id, Symbol: sym.Name, PriceType: pt, Direction: dir,
		TargetPrice:    engine.Price(base),
		ValidFrom:      req.GetValidFromUnixNanos(),
		Expires:        req.GetExpiresUnixNanos(),
		AutoDeactivate: req.GetAutoDeactivate(),
		Dims:           engine.Dims(vv, tv),
	}
```

- [ ] **Step 3: Build and test**

Run: `go vet ./... && go test ./internal/service/ -race -count=1`
Expected: PASS (all packages compile; `Meta` is simply zero-valued now).

- [ ] **Step 4: Commit**

```bash
git add internal/service/core.go
git commit -m "refactor(service): stop filling engine alert metadata"
```

---

### Task 2: Engine deletes AlertMeta and the meta map

**Files:**
- Modify: `engine/engine.go:66` (field), `engine/engine.go:88-101` (type), `engine/engine.go:167-169` (struct fields), `engine/engine.go:200` (init)
- Modify: `engine/index.go:134` (delete site), `engine/index.go:207` (delete site), `engine/index.go:233,236` (write site)
- Modify: `engine/engine_test.go:384` (test fill)

**Interfaces:**
- Consumes: Task 1's service (no `engine.AlertMeta` references outside `engine/` — verify in Step 1)
- Produces: `engine.AlertSpec` without a `Meta` field; `Engine` without `meta`. Nothing downstream references either.

No new test: the map has no readers, so no observable behavior changes. The guard is the full engine suite — conformance, oracle, parity, goleak — staying green, unmodified.

- [ ] **Step 1: Confirm no external references remain**

Run: `grep -rn "AlertMeta" --include="*.go" . | grep -v ^./engine/`
Expected: no output. If anything appears, that file must drop its reference before this task can compile.

- [ ] **Step 2: Delete the type and field in `engine/engine.go`**

From `AlertSpec` (line 66) delete:

```go
	Meta           AlertMeta      // cold data, stored verbatim
```

Delete the entire type (lines 88-101):

```go
// AlertMeta is the cold record: everything the notification pipeline needs
// after a trigger fires. Never touched by Match.
type AlertMeta struct {
	ID          AlertID
	Symbol      string
	UserID      string
	Segment     string
	Channels    []string
	Notes       string
	CreatedAt   int64 // unix nanos
	PriceType   PriceType
	Direction   Direction
	TargetPrice Price
}
```

In the `Engine` struct, change:

```go
	mu   sync.Mutex // guards refs, meta, live
	refs map[AlertID]*alertRef
	meta map[AlertID]*AlertMeta
	live uint64
```

to:

```go
	mu   sync.Mutex // guards refs, live
	refs map[AlertID]*alertRef
	live uint64
```

In `New`'s struct literal, delete:

```go
		meta:     make(map[AlertID]*AlertMeta),
```

- [ ] **Step 3: Delete the maintenance sites in `engine/index.go`**

Flusher removal path (around line 134) — change:

```go
			e.mu.Lock()
			if r, ok := e.refs[m.e.id]; ok && r.e.idx == m.e.idx {
				delete(e.refs, m.e.id)
				delete(e.meta, m.e.id)
				e.live--
			}
			e.mu.Unlock()
```

to:

```go
			e.mu.Lock()
			if r, ok := e.refs[m.e.id]; ok && r.e.idx == m.e.idx {
				delete(e.refs, m.e.id)
				e.live--
			}
			e.mu.Unlock()
```

Also update the comment above it: `clean refs/meta/live only` → `clean refs/live only`.

Upsert replace path (around line 207) — change:

```go
		delete(e.refs, a.ID)
		delete(e.meta, a.ID)
		e.live--
```

to:

```go
		delete(e.refs, a.ID)
		e.live--
```

Upsert publish path (around lines 233-236) — change:

```go
	meta := a.Meta
	e.mu.Lock()
	e.refs[a.ID] = &alertRef{sid: sid, e: ent}
	e.meta[a.ID] = &meta
	e.live++
	e.mu.Unlock()
```

to:

```go
	e.mu.Lock()
	e.refs[a.ID] = &alertRef{sid: sid, e: ent}
	e.live++
	e.mu.Unlock()
```

- [ ] **Step 4: Delete the test fill in `engine/engine_test.go`**

In `testSpec` (around line 384), change:

```go
	return AlertSpec{
		ID: AlertID{id}, Symbol: sym, PriceType: pt, Direction: dir,
		TargetPrice: price, ValidFrom: 1, AutoDeactivate: true,
		Meta: AlertMeta{ID: AlertID{id}, Symbol: sym},
	}
```

to:

```go
	return AlertSpec{
		ID: AlertID{id}, Symbol: sym, PriceType: pt, Direction: dir,
		TargetPrice: price, ValidFrom: 1, AutoDeactivate: true,
	}
```

- [ ] **Step 5: Build, vet, and run the full suite**

Run: `go vet ./... && go test ./... -race -count=1`
Expected: PASS, all packages. A compile error here means a missed reference — fix it, do not re-add the field.

- [ ] **Step 6: Commit**

```bash
git add engine/engine.go engine/index.go engine/engine_test.go
git commit -m "refactor(engine): drop write-only AlertMeta payload and map"
```

---

### Task 3: Verify the memory win at 1M alerts

**Files:** none modified — measurement only.

**Interfaces:** none.

- [ ] **Step 1: Rebuild and seed a fresh 1M store**

```bash
go build -o ./chronod ./cmd/chronod && go build -o ./chronofeed ./scripts/chronofeed
rm -f /tmp/meta-1m.bbolt
./chronofeed -db /tmp/meta-1m.bbolt -alerts 1000000 &
# wait for the "seeded alert ladder" log line, then kill the feeder (it would
# otherwise wait for chronod and start streaming)
```

- [ ] **Step 2: Start chronod, force GC, read the numbers**

```bash
nats-server -p 4222 &       # boot dependency
./chronod -db /tmp/meta-1m.bbolt &
# wait for the "alert store replay" log line
curl -s "http://localhost:8080/debug/pprof/heap?gc=1" > /dev/null
sleep 3
curl -s localhost:8080/stats | python3 -c "import json,sys; d=json.load(sys.stdin); print('heap=%.0fMB rss=%.0fMB' % (d['sys']['heap_bytes']/2**20, d['sys']['rss_bytes']/2**20))"
```

Expected: heap ≈ **470 MB** (was 628 MB at 1M on 2026-09-05) — the ~160 MB meta cost gone. Optionally confirm via `go tool pprof -sample_index=inuse_space -top ./chronod <heap profile>` that no `engine.Upsert`-side meta allocations appear.

- [ ] **Step 3: Stop the processes and clean up**

```bash
kill -INT <chronod-pid>; pkill -x nats-server; rm -f /tmp/meta-1m.bbolt
```

No commit (no file changes). If the heap number does not drop by ~160 MB, report it — do not paper over it.
