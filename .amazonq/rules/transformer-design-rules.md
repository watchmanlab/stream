# Transform Design Rules

## When to Add a Builtin Transform

A transformer should be builtin ONLY if it meets ALL criteria:

1. **Pipeline-focused**: Operates ON the pipeline itself, not BETWEEN pipelines
2. **Branch-agnostic**: Works on any stream without knowing about other streams
3. **Fundamental**: Cannot be composed from existing transformers
4. **Single responsibility**: Does one thing related to stream transformation

## When to Use Composition Instead

Use composition (don't add builtin) if:

- Routes/connects between streams → use `branch`, `merge`
- Combines existing transformers → let users compose
- Needs knowledge of other streams → composition pattern

## When to Add Events

Add `.events` property ONLY if transformer has:

- **Hidden internal state** that users need to observe
- **Error handling** (expected/unexpected errors)
- **Lifecycle events** (start/stop/state changes)
- **Internal decisions** (dropped values, buffering, concurrency limits)

Don't add events if:

- Transform is pure pipeline transformation
- All behavior is observable by listening to output
- No hidden internals to expose

## Examples

**Good builtins** (fundamental, pipeline-focused):

- `filter` - can't compose, operates on pipeline
- `map` - can't compose, operates on pipeline
- `throttle/audit` - temporal control, can't compose
- `each` - blocking side effect, fundamental
- `effect` - non-blocking side effect, fundamental

**Use composition** (not builtin):

- `auditTo(stream)` → `branch` + `audit`
- `filterMap` → `filter` + `map`
- `tapAsync` → `effect`

**Events needed**:

- `each/effect/consumer` - expose errors
- `queue` - expose buffering state
- `concurrent` - expose concurrency limits

**Events NOT needed**:

- `audit/throttle` - pure filtering, no hidden state
- `filter` - pure transformation
- `map` - pure transformation

## Decision Framework

```
New transformer idea?
  ├─ Can be composed? → YES → Don't add, document composition
  ├─ Operates between streams? → YES → Use branch/merge
  ├─ Branch-agnostic? → NO → Use composition
  └─ Fundamental primitive? → YES → Consider adding

Need events?
  ├─ Has hidden internals? → YES → Add events
  └─ Pure transformation? → NO → No events needed
```

## Philosophy

**Primitives over policies**: Provide minimal, orthogonal primitives that compose into complex behaviors. Keep API surface small, power through composition.
