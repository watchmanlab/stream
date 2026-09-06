# Pull-on-Push: How I Built a Reactive Library from First Principles After 300 Iterations

Two years. Three hundred iterations from scratch. A journey that started with a simple push-based reactive library and ended with something I hadn't seen described anywhere — a protocol I call **pull-on-push**.

This is the story of what I learned, what I threw away, and why the final design is the way it is.

---

## The Problem With Every Approach I Tried

### Iteration 1: Push-Based

The first version was straightforward: register a callback, push broadcasts to all listeners. Chaining streams is just subscribing to one and pushing to the next.

The problem? The producer dictates the rate. Backpressure is an afterthought. You end up adding buffers, and once you add buffers you've essentially built a pull-based system anyway — just a worse one.

### Iteration 2: Pull-Based with Async Generators

Eight months on this one. The code is beautiful. Lazy by default, memory-efficient in isolation. But the moment you need to bridge to push — which you always do for real-world sources like WebSockets or DOM events — you need queues and intermediate promises. Every transformer adds another level of nesting. Performance collapses. Discarded.

### Iteration 3: Pull-on-Push

The insight that changed everything: instead of *requesting values*, you *register a callback and request its execution*.

This is the difference. In a pull system, you call `next()` and get a value back. In pull-on-push, you register your handler once at consumer creation and call `next()` to grant *credit* — permission for the next value to be delivered. The handler is called by whoever has a value ready: either the upstream pushing one, or the credit system draining the queue.

---

## The Credit System

The `Consumer` is the fundamental unit. It has a handler registered at construction time (immutable, JIT-friendly) and a credit counter.

```typescript
const consumer = new Consumer<number>((self, value) => {
  console.log(value);
  self.next(); // grant credit for the next value
});
consumer.next(); // grant initial credit
consumer.push(1); // handler fires immediately — credit was available
consumer.push(2); // handler fires immediately — self.next() re-granted credit
```

When `push` is called:
- If credit > 0 and the queue is empty → handler fires immediately, credit decrements
- Otherwise → value is enqueued

When `next` is called:
- If the queue has values → drain them at the consumer's own pace
- If the queue is empty → signal upstream to produce more

This means a synchronous consumer always has positive credit before any value arrives. The queue is never touched. The hot path is identical to a push-based system — zero overhead.

An async consumer naturally buffers in the queue and drains at its own pace. Backpressure is implicit and free.

---

## Primitives Must Be Dumb

One of the hardest-won lessons: **the core must know nothing about errors, promises, or any special values**.

Every time I made the core "aware" of something — promise-aware, error-aware — I hit a wall. Branching multiplied. Implementation variants appeared. The hot path paid a tax for cases that might never occur.

The solution: keep `Consumer`, `Stream`, and `Source` completely agnostic. Handle everything in dedicated transformers.

**Error handling?** The `safe` transformer wraps a pipeline in try/catch and emits `Error<thrown>` as a value. The core never sees it.

**Async workflows?** The `resolve` transformer handles promise resolution with configurable concurrency. The core never sees a promise.

**Bounded queues?** The `Consumer` accepts a `queueFactory`. Transformers like `latest(n)` provide a `DefaultSizedQueue` internally. The core never enforces a bound.

```typescript
// Error handling is just a transformer
of(1, 2, 3)
  .pipe(safe(map(v => { if (v === 2) throw 'bad'; return v; })))
  .pipe(listen(console.log)); // 1, Error('bad'), 3

// Async is just a transformer
fromGenerator(function* () {
  yield fetch('/a');
  yield fetch('/b');
}).pipe(resolve(2)).pipe(listen(console.log));
```

---

## The Three Primitives

The entire user-facing API is three concepts:

**`Stream` (or `Source`, or `Consumable`)** — anything that produces values. These three names refer to the same primitive at different layers: `Consumable` is the interface (anything with a `consume` method), `Source` is the abstract base that adds `pipe` and `for await...of`, and `Stream` is the concrete multicast implementation. From the user's perspective they are interchangeable — if it has `consume`, it participates in the pipeline.

```typescript
const stream = new Stream<number>();
stream.$firstConsumerJoin.pipe(listen(() => console.log('first consumer joined')));
stream.consume((c, v) => { console.log(v); c.next(); }).next();
stream.push(1).push(2).push(3);
stream.terminate('complete');
```

**`pipe`** — the composition mechanism. Chains read left to right. `source.pipe(fn)` is just `fn(source)`. Transformers are not special — they are plain `Source`s whose constructor accepts another `Consumable` as input. `map`, `filter`, `debounce` are all just `Source` subclasses. `pipe` is just the glue that passes one into the next.

```typescript
fromInterval(100)
  .pipe(take(5))
  .pipe(map(v => v * 2))
  .pipe(filter(v => v > 4))
  .pipe(listen(console.log)); // 6, 8
```

---

## Active vs Passive Consumers

Some sources are *replayable* — each new consumer gets a fresh sequence (iterables, intervals, generators). Others are *shared* — all consumers see the same values (a `Stream` after `share()`).

When a source is shared, who triggers production? By default, every consumer is **active** — the fastest one drives the source, and slower ones buffer values in their queues. This preserves the hot path: the fast consumer never waits.

But sometimes you want a consumer that observes without driving — a logger, a debugger, a side-channel. That's what `passive` is for:

```typescript
const shared = fromInterval(500).pipe(share());

// This drives the source
shared.pipe(listen(v => console.log('main:', v)));

// This observes without driving — only receives values when the active consumer pulls
shared.pipe(passive()).pipe(listen(v => console.log('observer:', v)));
```

The `passive` transformer simply removes the `next` callback from the consumer options. One line of code. No special-casing in the core.

---

## Aggregate Operators Are Decoupled From Collection

`sum`, `max`, `min`, `count`, `scan` — all emit *running values* after each input. They don't wait for completion. To get the final result, you pipe `last()` after them.

```typescript
of(1, 2, 3, 4, 5)
  .pipe(scan(0, (acc, v) => acc + v))
  .pipe(listen(console.log)); // 1, 3, 6, 10, 15

of(1, 2, 3, 4, 5)
  .pipe(scan(0, (acc, v) => acc + v))
  .pipe(last())
  .pipe(listen(console.log)); // 15
```

This is not an accident. Decoupling the computation from the collection means you can observe intermediate state, add `take` or `filter` mid-stream, or compose with other operators. `last` is just another transformer that happens to collect the final value.

---

## The Full Transformer Catalog

The library ships with a complete set of operators, all built on the same `Consumer`/`Source` primitives:

**Filtering & slicing:** `filter` (with a free `$complements` stream for rejected values), `take`, `skip`, `takeWhile`, `takeUntil`, `skipWhile`, `skipUntil`, `first`, `last`, `distinct`

**Transformation:** `map`, `tap`, `tapInput`, `tapTerminate`, `scan`, `scanArray`, `context`

**Flattening:** `flat` (arrays), `flat$` (sequential inner consumables), `switch$` (latest inner consumable, cancels previous)

**Combination:** `merge`, `zip`, `combine`

**Timing:** `debounce`, `pace`, `delay`, `delayOnce`

**Windowing:** `buffer` (sliding window), `batch` (non-overlapping chunks)

**Sharing & routing:** `share`, `passive`, `gate`, `pump`

**Async:** `resolve` (promise resolution with concurrency)

**Error handling:** `safe`

**Lifecycle:** `terminate`, `scope`, `scopeStrict`

**Aggregation:** `sum`, `min`, `max`, `count`, `every`, `range`, `latest`

---

## Built-In Sources

```typescript
of(1, 2, 3)                          // fixed values
fromIterable([1, 2, 3])              // any iterable
fromGenerator(function* () { ... })  // generator function
fromInterval(500)                    // setInterval counter
fromTimeout(1000, 'done')            // setTimeout, single value
fromRange(1, 10)                     // integer range
fromEventTarget(window, 'click')     // DOM events
fromAbortSignal(controller.signal)   // AbortSignal
fromFunction(() => Math.random())    // single function call
```

And two specialized `Stream` subclasses:

- `Signal<VALUE>` — auto-terminates after the first push. For one-shot events.
- `State<VALUE>` — has a `.value` property; setting it pushes to all consumers.

---

## Termination Is a First-Class Concept

Every consumer and stream has two termination modes:

- `"abort"` — stop immediately, clear all queues
- `"complete"` — drain remaining queued values at the consumer's own pace, then stop

Both `Consumer` and `Stream` implement `Symbol.dispose` (abort) and `Symbol.asyncDispose` (complete), so they work with the `using` / `await using` keywords natively.

```typescript
{
  using stream = new Stream<number>();
  // stream.terminate('abort') called automatically at block exit
}

{
  await using stream = new Stream<number>();
  // stream.terminate('complete') called, waits for drain
}
```

---

## What This Looks Like in Practice

A real-world pipeline: fetch a list of URLs with concurrency control, debounce a search input, and share results across multiple consumers.

```typescript
const search$ = fromEventTarget(input, 'input')
  .pipe(map(e => e.target.value))
  .pipe(debounce(300))
  .pipe(map(query => fetch(`/api/search?q=${query}`)))
  .pipe(resolve(1))
  .pipe(share());

// Main results
search$.pipe(listen(results => renderResults(results)));

// Passive logger — never drives the fetch
search$.pipe(passive()).pipe(listen(v => console.log('result received')));
```

Or a bounded sliding window over a live stream:

```typescript
fromInterval(100)
  .pipe(share())
  .pipe(latest(5))   // always have the last 5 values buffered for late subscribers
  .pipe(buffer(3))   // emit sliding windows of 3
  .pipe(listen(console.log));
```

---

## Why Not RxJS?

RxJS is excellent and battle-tested. This library is not a replacement — it's a different set of trade-offs:

- No `Observable` wrapper. The `Consumable` interface is the contract. Anything with a `consume` method participates.
- No scheduler abstraction. The credit system handles backpressure natively without a separate concept.
- Error handling is opt-in via `safe`, not baked into every operator.
- The core is ~200 lines. Every transformer is self-contained and independently testable.
- Full TypeScript inference through the entire pipe chain, including type-narrowing in `filter`.

---

## The Distilled Principle

After 300 iterations, the principle that survived everything:

> **Primitives must be dumb and unbounded. Behavior lives in transformers.**

Every time I violated this — making the queue bounded by default, making the core error-aware, making the protocol promise-aware — complexity multiplied and the hot path suffered.

Every time I respected it — moving error handling to `safe`, moving queue bounds to `latest`/`batch`, moving async to `resolve` — the core got simpler and the system got more composable.

The three primitives (`stream`, `pipe`, `transformers`) can express anything. The rest is just transformers all the way down.

---

*`@soffinal/stream` is available on JSR and npm. TypeScript 5.x, zero runtime dependencies, ESM only.*
