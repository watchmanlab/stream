# I Spent Two Years Building a Reactive Library. Here's What I Learned.

I want to tell you about a problem that took me two years and more than three hundred iterations from scratch and from first principles to solve properly.

Not because the problem is hard to describe — it isn't. But because every time I thought I had it, I'd discover a new wall. And the walls kept teaching me something.

The problem: **how do you connect a producer and a consumer when they run at different speeds?**

That's it. That's the whole thing. Everything in reactive programming — backpressure, buffering, scheduling, error handling — is downstream of that one question.

---

## Start With What You Know

You've probably used something like this before:

```typescript
eventEmitter.on("data", (value) => {
  console.log(value);
});
```

Simple. The producer calls your callback whenever it has something. You receive it. Done.

Now what happens when your callback is slow? Maybe it's doing a database write, or a network request. The producer doesn't know and doesn't care — it keeps firing. Your callback gets called again before the previous one finished. Values pile up. Memory grows. Things break.

This is the **backpressure problem**. The producer is dictating the rate, and the consumer has no say.

The classic fix is to add a buffer. But now you have a new problem: how big should the buffer be? What happens when it fills up? Do you drop values? Block the producer? Which values do you drop — the oldest or the newest?

Every answer spawns more questions. And every answer you bake into the core of your library becomes a constraint that someone, somewhere, will need to work around.

I know this because I tried all of them.

---

## My First Attempt: Pure Push

My first reactive library was push-based. Register a callback, push broadcasts to all listeners. Chaining is just subscribing to one stream and pushing to the next.

```typescript
// What I built first
const stream = new Stream<number>();
stream.listen((v) => console.log(v));
stream.push(1); // fires immediately
```

It worked. It was fast. The hot path was just a function call.

But the moment I needed to handle a slow consumer, I was stuck. The producer didn't know the consumer was slow. I had to add buffering. And once I added buffering, I was essentially building a pull system on top of a push system — the worst of both worlds.

---

## My Second Attempt: Async Generators

Eight months on this one. The code was genuinely beautiful.

```typescript
async function* map(source, fn) {
  for await (const value of source) {
    yield fn(value);
  }
}
```

Lazy by default. Memory-efficient. The consumer controls the pace by simply not calling `next()` on the iterator.

But then I needed to bridge to push sources — DOM events, WebSockets, anything that fires on its own schedule. To do that, you need a queue and a promise that resolves when the queue has something. Every value crosses an async boundary. Every transformer adds another level of nesting generators.

The performance was terrible. And the sequential nature of generators made fan-out (one producer, many consumers) genuinely painful.

Eight months. Discarded.

---

## The Insight That Changed Everything

I kept going back to the same question: what does a consumer actually need?

It needs to say: _"I'm ready for the next value."_

In a pull system, that's `iterator.next()` — you call it and get a value back.

In a push system, that's implicit — the producer decides when you're ready.

What if I separated those two things? What if the consumer could say _"I'm ready"_ without that being a request for a value? And what if the producer could push a value without knowing whether the consumer is ready?

That's the credit system.

---

## The Credit System

Here's the core idea. A consumer has a **credit counter**. Calling `next()` increments it by one. When a value is pushed to the consumer:

- If credit > 0 and the queue is empty → the handler fires immediately, credit decrements
- Otherwise → the value goes into a queue

When `next()` is called and the queue has values → drain them at the consumer's own pace.

```typescript
const consumer$ = new Consumer<number>((self$, value) => {
  console.log(value);
  self$.next(); // "I'm ready for the next one"
});

consumer$.next(); // grant initial credit
consumer$.push(1); // fires immediately — credit was available
consumer$.push(2); // fires immediately — self.next() re-granted credit
```

Now watch what happens with a slow consumer:

```typescript
const consumer$ = new Consumer<number>((self$, value) => {
  // simulate async work
  setTimeout(() => {
    console.log(value);
    self$.next(); // only ready after the work is done
  }, 1000);
});

consumer$.next(); // grant initial credit
consumer$.push(1); // fires immediately
consumer$.push(2); // no credit yet — goes into the queue
consumer$.push(3); // no credit yet — goes into the queue
// after 1 second: logs 1, then drains 2, then 3 at 1s intervals
```

The producer doesn't need to know anything about the consumer's speed. The consumer controls its own throughput. Backpressure is implicit and free.

And here's the key property: **any consumer that has already called `next()` before a value arrives will fire immediately** — the queue is never touched. The hot path is a direct function call, identical to a raw push-based system.

In a shared stream with multiple consumers, the one that calls `next()` first drives the source. When the source pushes a value, every other consumer that already has credit fires immediately too — no buffering, no delay. The queue only comes into play for a consumer that hasn't called `next()` yet when the value arrives. "Slow" here has nothing to do with async — it just means _called `next()` after the value was already pushed_.

---

## Pull-on-Push

I call this protocol **pull-on-push** because it's pull-based (the consumer controls the rate) but disguised as push (values are delivered by the producer). The consumer registers its handler once at creation — not on each `next()` call. This matters for performance: the JIT can optimize a stable function reference the same way it optimizes a push-based listener.

The protocol is the same shape as an iterator — you call `next()` to advance — but fundamentally different. You're not requesting a value. You're granting permission for the next value to be delivered whenever it arrives.

This single insight resolves the tension between push and pull that I'd been fighting for two years.

### Reentrancy for Free

Callback-based synchronous systems have a classic trap: reentrancy. If calling `next()` inside a handler triggers another `push`, which calls the handler again before the first call returns, you get a stack overflow or corrupted state.

The credit variable solves this without any extra machinery. Look at the drain loop in `next()`:

```typescript
next(): this {
  this._credit++;

  if (!this._queue?.size) {
    this._options.next?.(this);
    return this;
  }

  if (this._credit > 1) return this; // already draining — just increment and exit

  while (this._credit > 0) {
    const value = this._queue.dequeue();
    this._handler(this, value);
    this._credit--;
  }
}
```

If `next()` is called re-entrantly from inside the handler — say, the handler calls `next()` synchronously before returning — credit goes from 1 to 2. But the guard `if (this._credit > 1) return this` exits immediately. The outer `while` loop is still running and will pick up the extra credit on its next iteration. No recursion. No locks. No special cases. The credit counter is the entire reentrancy solution.

### One Consumer Per Transformer — The Transducer Pattern

Here's something that surprised me when I worked it out.

The naive way to implement `map` is: create a new consumer for the output, consume from the input, transform each value, push it into the output consumer, return the output consumer. Two consumers per transformation.

But look at what `map` actually does:

```typescript
consume(handler, options) {
  return this.$input.consume(
    (c, v) => handler(c, this.mapper(v, index++)),
    options
  );
}
```

It consumes from the input and returns _that same consumer_ — the one created by the input — with the handler wrapped. There is no second consumer. The transformation happens inside the callback, and the consumer that `next()`, `terminate()`, and `push()` operate on is the one that came from the input.

The caller doesn't care which consumer it gets back. It just calls `next()` and `terminate()` on it — neither of those is value-type-aware. So we can safely return the input's consumer with a wrapped handler.

Chain a few transformers together:

```typescript
of(1, 2, 3)
  .pipe(filter((v) => v > 1)) // wraps handler
  .pipe(map((v) => v * 10)) // wraps handler again
  .pipe(listen(console.log)); // the final handler
```

What actually executes when a value arrives is a single chain of nested function calls — no intermediate consumers, no intermediate allocations. The JIT sees stable, inlineable function references at every level. This is exactly what transducers give you in functional programming, but it falls out naturally from the protocol.

This collapses the moment you use `share()` — sharing requires a real multicast `Stream` with independent consumers. But for the common case of a linear pipeline, you get transducer-level performance for free, without thinking about it.

This is also why many transformers that _could_ be expressed in terms of others are instead written from scratch. `range` could be written as `skip(start).pipe(take(offset))` — elegant, readable, two lines. But that would create two consumers, two handler wrappers, two objects in the chain. Written directly against the input, it's one consumer, one handler, zero intermediate allocations. The composability of the API is a user-facing property. The implementation doesn't have to pay for it.

---

## Three Primitives, That's It

The entire user-facing API is three concepts. Once you understand them, everything else follows.

**`Stream` / `Source` / `Consumable`** — one primitive, three layers. `Consumable` is the interface: anything with a `consume` method participates in the pipeline. `Source` is the abstract base that adds `pipe` and `for await...of`. `Stream` is the concrete multicast implementation. From the user's perspective they are interchangeable — if it has `consume`, it's in the pipeline.

> **Naming convention:** any variable holding a `Consumable` (a source, stream, or transformer) is prefixed with `$`. Any variable holding a `Consumer` (the object returned by `consume`) is suffixed with `$`. So `$input` is something you can consume from, and `input$` is the consumer you got back from consuming it. You'll see this throughout the library's own source code and it's worth adopting in your own pipelines — it makes the direction of data flow immediately visible at a glance. When you open any transformer's source, you'll instantly know which variables are producers and which are consumers without reading a single comment.

**`pipe`** — the composition mechanism. `source.pipe(fn)` is literally `fn(source)`. That's the entire implementation. Transformers are not special — they are plain `Source`s whose constructor accepts another `Consumable` as input. `map`, `filter`, `debounce` are all just `Source` subclasses. `pipe` is the glue that passes one into the next.

```typescript
// source.pipe(map(fn)) is literally map(fn)(source)
// which returns a new Map instance, which is a Source
// which you can pipe again

of(1, 2, 3, 4, 5)
  .pipe(filter((v) => v % 2 === 0)) // returns a Filter (a Source)
  .pipe(map((v) => v * 10)) // returns a Map (a Source)
  .pipe(listen(console.log)); // 20, 40
```

There's no framework magic. No observable wrapper. No scheduler. Just objects that implement `consume`.

### Transformers Are Classes — The Function Factory Is Just Convenience

Every transformer is a class. `map`, `filter`, `debounce` — all classes that extend `Source`. The function you call in `.pipe()` is just a factory that returns an instance:

```typescript
// These two are identical
of(1, 2, 3).pipe(map((v) => v * 2));
new Map(of(1, 2, 3), (v) => v * 2);
```

The reason they're classes and not plain functions is memory and CPU. A class instance has a fixed shape that the JIT can optimize aggressively — it can inline the `consume` call, cache the property lookups, and treat the whole thing as a monomorphic call site. A plain closure-based approach would produce a new object shape on every call, defeating those optimizations.

The function factory (`map(fn)`) exists purely for `.pipe()` ergonomics. But because the underlying thing is a class, you can instantiate transformers directly. This is useful when you want step-by-step imperative code instead of a chain:

```typescript
// Chained style
of(1, 2, 3)
  .pipe(filter((v) => v > 1))
  .pipe(map((v) => v * 10))
  .pipe(listen(console.log));

// Step-by-step style — same result, easier to debug or build dynamically
const $source = of(1, 2, 3);
const $filtered = new Filter($source, (v) => v > 1);
const $mapped = new Map($filtered, (v) => v * 10);
$mapped.pipe(listen(console.log));
```

Both styles produce exactly the same pipeline. The step-by-step style is particularly useful when you need to conditionally add a transformer, hold a reference to an intermediate stage, or build a pipeline programmatically.

---

## Why the Core Must Stay Dumb

Here's a design decision that took me a long time to arrive at, and that I think is the most important one in the library.

**The core knows nothing about errors. Nothing about promises. Nothing about bounded queues.**

Every time I made the core "aware" of something special, complexity multiplied. Let me show you why with errors.

### The Error Problem

In most reactive libraries, errors are a first-class concept. Every operator has to handle the error channel. Every subscription has an error callback. The type system carries the error type everywhere.

The problem: most of your code doesn't throw. You're paying the cost of error-awareness on every single value, in every single operator, for a case that might never happen.

My answer: errors are just values. The core doesn't know what an `Error` is. If you want error handling, you add a transformer that wraps your pipeline in a try/catch and emits `Error<thrown>` as a value when something throws.

```typescript
of(1, 2, 3)
  .pipe(
    safe(
      map((v) => {
        if (v === 2) throw "something went wrong";
        return v;
      }),
    ),
  )
  .pipe(listen(console.log));
// 1, Error('something went wrong'), 3
```

The `safe` transformer catches the throw and emits it as a typed value. Downstream, you can filter it out, log it, retry — whatever you need. The core never saw it.

### The Async Problem

Same principle applies to promises. Most reactive libraries have special handling for async operators. You pass an async function to `map` and it... does something. Maybe it waits. Maybe it runs concurrently. The behavior is implicit.

My answer: promises are just values. If you have a stream of promises, pipe `resolve` to handle them. You control the concurrency explicitly.

```typescript
fromGenerator(function* () {
  yield fetch("/api/users");
  yield fetch("/api/posts");
  yield fetch("/api/comments");
})
  .pipe(resolve(2)) // max 2 in-flight at once
  .pipe(listen(console.log));
```

`resolve(2)` means: keep at most 2 promises in flight. When one resolves, pull the next one. Rejections come through as `Error<reason>` values — same as synchronous errors. The core never saw a promise.

### The Bounded Queue Problem

The consumer's queue is unbounded by default. I get asked about this a lot.

My answer: the queue is bounded by transformers, not by configuration. If you want to drop values when a consumer is too slow, you add a transformer that does that. If you want to emit the last N values to late subscribers, you add `latest(n)`. If you want non-overlapping chunks, you add `batch(size)`.

```typescript
// Late subscriber gets the last 3 values immediately, then live values
const $shared = fromInterval(100).pipe(share()).pipe(latest(3));
setTimeout(() => $shared.pipe(listen(console.log)), 1000);

// Slow consumer gets values in batches of 10
$fastSource.pipe(batch(10)).pipe(listen(processBatch));
```

Each transformer is responsible for exactly one strategy. The core stays clean. You compose the behavior you need.

---

## Active vs Passive Consumers

Here's a concept that doesn't exist in most reactive libraries, but that comes up constantly in practice.

When you share a source across multiple consumers, who drives it? By default, every consumer is **active** — the first one to call `next()` triggers production, and the value is broadcast to all consumers. Any consumer that already has credit fires immediately. Only a consumer that hasn't called `next()` yet when the value arrives will queue it — and even then, it drains as soon as it does call `next()`.

But sometimes you want a consumer that observes without driving at all. A logger. A debugger. A metrics collector. You don't want it to trigger a database query or a network request just because it subscribed.

```typescript
const $shared = fromInterval(500).pipe(share());

// This drives the source — triggers the interval
$shared.pipe(listen((v) => console.log("main:", v)));

// This observes without driving — only receives values when the active consumer pulls
$shared.pipe(passive()).pipe(listen((v) => console.log("observer:", v)));
```

`passive()` is a one-line transformer that removes the `next` callback from the consumer options. That's it. No special-casing in the core. The behavior emerges from the protocol.

---

## Concepts Worth Knowing

### `filter.$complements` — The Rejected Values Stream

`filter` doesn't just pass or drop values. Rejected values go to a lazy `$complements` stream that costs nothing if unused. This lets you route values to two different pipelines from a single pass:

```typescript
const $even = of(1, 2, 3, 4, 5).pipe(filter((v) => v % 2 === 0));

$even.pipe(listen(console.log)); // 2, 4
$even.$complements.pipe(listen(console.log)); // 1, 3, 5
```

The `$complements` stream is passive relative to `filter` — consuming it never drives the source. Values only appear there when the main pipeline is being consumed.

`tapInput` is the tool for accessing these side-channel properties mid-pipe:

```typescript
$source
  .pipe(tapInput((f) => f.$complements.pipe(listen(logRejected))))
  .pipe(filter(isValid))
  .pipe(listen(process));
```

### `zip.$rest` — Unmatched Values on Termination

`zip` pairs values from multiple sources into tuples, emitting only when all inputs have contributed. If the source terminates with unmatched buffered values, they're available on `$rest`:

```typescript
const $z = s1.pipe(zip($s2));
$z.$rest.pipe(listen(console.log)); // any unmatched values when one source ends early
$z.pipe(listen(console.log)); // [s1val, s2val] tuples
```

### `combine` vs `zip` vs `combineLatest`

These three cover different combination needs:

- `zip` — waits for all inputs to have a new value, emits a tuple. Strict pairing.
- `combine` — emits on any input using the last known value (or `EMPTY`) for the others. No waiting.
- `combineLatest` — not a separate operator. You get it by piping `latest(1)` to each input before `zip`:

```typescript
$s1
  .pipe(latest(1))
  .pipe(zip($s2.pipe(latest(1))))
  .pipe(listen(console.log));
```

This is the philosophy in action: behavior lives in transformers, not in operator variants.

### `distinct` — Full Deduplication, Not Just Consecutive

`distinct` uses a `Set` to track all seen values — not just the previous one. It also accepts a `keySelector` for object identity and a `$flushes` notifier to reset the seen-set:

```typescript
// Deduplicate by id
$stream.pipe(distinct((v) => v.id)).pipe(listen(console.log));

// Reset deduplication every time a page changes
$stream.pipe(distinct((v) => v.id, pageChange$)).pipe(listen(console.log));
```

### `pump` — Waking a Dormant Pipeline

By default, a pipeline is dormant — nothing runs until a consumer calls `next()`. `pump` is a trigger: it wakes the pipeline up immediately without waiting for any downstream subscriber to exist.

The key thing to understand is that `pump` doesn't control the rate — it just removes the requirement for a downstream consumer before the pipeline starts. The rate is still entirely dictated by whatever transformers are upstream. If there's a `resolve(2)`, values come through at the rate `resolve` allows. If there's a `debounce`, values come through at the debounced rate. `pump` just fires the starting gun.

This also means you don't need `listen` at the end of a pipeline. Use `tap` for your side effects and `pump` to wake it up — the pipeline is self-contained and clean:

```typescript
// listen at the end works, but mixes side effects with the wake-up call
fromEventTarget(socket, "message")
  .pipe(map((e) => JSON.parse(e.data)))
  .pipe(resolve(2))
  .pipe(listen(updateUI));

// tap + pump: side effects stay in the pipeline, pump is just the trigger
fromEventTarget(socket, "message")
  .pipe(map((e) => JSON.parse(e.data)))
  .pipe(resolve(2))
  .pipe(tap(updateUI)) // side effect lives here
  .pipe(pump()); // just wakes it up
```

Anything you attach downstream of `pump()` is still a dormant consumer and needs its own wake-up call. You can use another `pump()`, or `tap + pump`, or `listen` — which is just a shorthand for `tap + pump`:

```typescript
const $hot = fromEventTarget(socket, "message")
  .pipe(map((e) => JSON.parse(e.data)))
  .pipe(resolve(2))
  .pipe(tap(updateUI))
  .pipe(pump()); // wakes the pipeline above

// these three are equivalent ways to wake the downstream
$hot.pipe(listen(console.log)); // shorthand for tap + pump
$hot.pipe(tap(console.log)).pipe(pump()); // explicit tap + pump
```

### `context` — Shared Data That Travels With the Pipeline

`context` wraps each value as `{ value, context }` where the same context object is shared across all values in the stream. Its purpose goes far beyond accumulating state like `scan` — it's a general-purpose carrier for anything that needs to be accessible at any point downstream without threading it through the values themselves.

Session data, request metadata, correlation IDs, feature flags, user permissions, timing information — anything that is relevant to the whole pipeline but orthogonal to the values flowing through it:

```typescript
// Attach session metadata to every event in the pipeline
fromEventTarget(socket, "message")
  .pipe(map((e) => JSON.parse(e.data)))
  .pipe(context({ userId: session.userId, requestId: crypto.randomUUID() }))
  .pipe(tap(({ value, context }) => logger.info(context.requestId, value)))
  .pipe(map(({ value, context }) => ({ ...value, userId: context.userId })))
  .pipe(resolve())
  .pipe(pump());

// Or accumulate state across values
of(1, 2, 3)
  .pipe(context({ total: 0 }))
  .pipe(
    tap(({ value, context }) => {
      context.total += value;
    }),
  )
  .pipe(listen(({ value, context }) => console.log(value, "total:", context.total)));
```

The context object is the same reference for every value — mutations made in one `tap` are visible in every step. This makes it a lightweight alternative to external state when the state is scoped to a single pipeline's lifetime.

### `delay(0)` — Microtask Scheduling

`delay` with `ms <= 0` uses `queueMicrotask` instead of `setTimeout`. This defers each value to the next microtask checkpoint without leaving the current event loop tick — useful for breaking synchronous chains without the overhead of a timer.

```typescript
of(1, 2, 3).pipe(delay(0)).pipe(listen(console.log)); // deferred via queueMicrotask
```

### `State.value` vs `State.push`

`State` has both a `.value` setter and an inherited `.push()` method. They behave differently intentionally: setting `.value` updates the stored value and pushes to consumers. Calling `.push()` directly notifies consumers without updating `.value`. This lets you emit transient events on a state stream without changing what `.value` returns:

```typescript
const $status = state<string>("idle");
$status.pipe(listen(console.log));

$status.value = "loading"; // logs 'loading', status.value === 'loading'
$status.push("ping"); // logs 'ping', but status.value still === 'loading'
```

### Replayable Sources

Some sources are **replayable** — each new consumer gets a fresh independent sequence. `of`, `fromIterable`, `fromGenerator`, `fromInterval`, `fromRange`, `fromTimeout`, `fromFunction` are all replayable. This means:

```typescript
const $source = fromIterable([1, 2, 3]);
$source.pipe(listen(console.log)); // 1, 2, 3
$source.pipe(listen(console.log)); // 1, 2, 3 again — fresh iterator
```

When you `share()` a replayable source, the first consumer to call `next()` starts the sequence and all consumers receive the same values from that point. Without `share()`, each consumer replays from the beginning independently.

**Critical gotcha:** if the replayable source is synchronous and the first consumer to call `next()` is also synchronous, it will drain the entire source before any other consumer gets a chance to receive anything. The source completes in the same tick, and late consumers see nothing:

```typescript
const $shared = of(1, 2, 3).pipe(share());

// This consumer is synchronous — it drains the entire source immediately
$shared.pipe(listen(console.log)); // 1, 2, 3

// This consumer subscribes after the source is already exhausted — gets nothing
$shared.pipe(listen(console.log)); // nothing
```

This only affects sources that are both replayable **and** synchronous — `of`, `fromIterable`, `fromRange`, `fromGenerator` with a synchronous generator. `fromInterval` is replayable too, but it's asynchronous — each consumer gets its own independent timer and the first consumer never drains it in the same tick, so sharing it is safe.

The fix is to register all consumers before any of them calls `next()`, or to use `latest(n)` after `share()` to buffer values for late subscribers.

---

## Aggregation: Decoupled From Collection

One more design decision worth explaining: aggregation operators emit running values, not final values.

`sum`, `max`, `min`, `count`, `scan` — they all emit after every input value. To get the final result, you pipe `last()` after them.

```typescript
of(1, 2, 3, 4, 5)
  .pipe(scan(0, (acc, v) => acc + v))
  .pipe(listen(console.log)); // 1, 3, 6, 10, 15

of(1, 2, 3, 4, 5)
  .pipe(scan(0, (acc, v) => acc + v))
  .pipe(last())
  .pipe(listen(console.log)); // 15
```

Why? Because decoupling the computation from the collection means you can observe intermediate state, add `take` or `filter` mid-stream, or compose with other operators. `last` is just another transformer. The aggregation operator doesn't need to know whether you want the final value or all of them.

---

## The Three Flattening Strategies

One area where the library's philosophy really shows is flattening. There are three operators for it, and they solve three genuinely different problems.

### `flat` — flatten arrays into individual values

The simplest case. You have a stream of arrays and you want individual values:

```typescript
of([1, 2], [3, 4]).pipe(flat()).pipe(listen(console.log)); // 1, 2, 3, 4
```

This is purely about arrays. It has nothing to do with streams inside streams.

### `flat$` — flatten inner streams sequentially

You have a stream of streams (or any `Consumable`), and you want to consume them one at a time — finish the first before starting the second:

```typescript
of(of(1, 2), of(3, 4)).pipe(flat$()).pipe(listen(console.log)); // 1, 2, 3, 4 — in order, sequentially
```

The key word is _sequentially_. `flat$` waits for each inner stream to complete before subscribing to the next one. If you have a stream of HTTP requests and you need them to execute in order, this is your operator.

### `switch$` — always consume the latest inner stream, cancel the previous

You have a stream of streams, but you only care about the most recent one. When a new inner stream arrives, the previous one is cancelled:

```typescript
const $inner1 = new Stream<number>();
const $inner2 = new Stream<number>();
const $outer = new Stream<any>();

$outer.pipe(switch$()).pipe(listen(console.log));

$outer.push($inner1);
$inner1.push(1); // logs 1
$outer.push($inner2); // inner1 is cancelled
$inner1.push(99); // ignored — inner1 was cancelled
$inner2.push(2); // logs 2
```

This is the operator behind "cancel the previous search request when a new one comes in". Combined with `debounce` and `map` to a fetch, it gives you exactly that behavior.

**Critical gotcha with `switch$`:** it does not work with replayable sources. If you do `of(of(1,2), of(3,4)).pipe(switch$())`, you get nothing. The reason: `switch$` is eager on the outer stream but lazy on the inner one. With a replayable source like `of`, the outer stream completes synchronously before `switch$` has a chance to attach a consumer to the last inner value. Use `switch$` with hot sources — `Stream`, `fromEventTarget`, etc.

---

## `merge`, `zip`, `combine` — Combining Multiple Sources

These three operators all take multiple sources and combine them, but they have very different semantics.

### `merge` — emit from all, terminate with the primary

`merge` subscribes to all inputs and emits values from whichever fires first. The important detail: **it terminates when the primary input (the one you piped from) terminates**, not when all inputs terminate.

```typescript
const $s1 = new Stream<string>();
const $s2 = new Stream<string>();

$s1.pipe(merge($s2)).pipe(listen(console.log));

$s1.push("from s1"); // logs 'from s1'
$s2.push("from s2"); // logs 'from s2'
$s1.terminate("complete"); // pipeline ends — s2 is also terminated
$s2.push("too late"); // never arrives
```

If you want the pipeline to survive until all inputs terminate, you need a different approach — combine `scope` or `scopeStrict` with `merge`.

### `zip` — strict pairing into tuples

`zip` waits until every input has contributed one new value, then emits a tuple. It's strict: if `s1` emits three times before `s2` emits once, the first two values from `s1` are buffered and waiting.

```typescript
$s1.pipe(zip($s2)).pipe(listen(console.log));
$s1.push(1); // buffered, waiting for s2
$s2.push("a"); // emits [1, 'a']
$s1.push(2); // buffered
$s1.push(3); // buffered
$s2.push("b"); // emits [2, 'b']
```

### `combine` — emit on any input, use last known for others

`combine` emits every time any input emits. For inputs that haven't emitted yet, it uses the `EMPTY` sentinel. For inputs that have emitted before, it uses their last known value.

```typescript
$s1.pipe(combine($s2)).pipe(listen(console.log));
$s1.push(1); // emits [1, EMPTY]  — s2 hasn't emitted yet
$s2.push("a"); // emits [1, 'a']   — uses last known s1 value
$s1.push(2); // emits [2, 'a']   — uses last known s2 value
```

### `combineLatest` — compose it yourself

There is no separate `combineLatest` operator. You build it by piping `latest(1)` to each input before `zip`. This gives you the same semantics — emit when any input emits, using the most recent value from each — but you compose it from primitives:

```typescript
$s1
  .pipe(latest(1))
  .pipe(zip($s2.pipe(latest(1))))
  .pipe(listen(console.log));
```

---

## `scope` and `scopeStrict` — Lifetime Management

Every pipeline needs a way to stop. You can call `terminate()` manually, but the cleaner pattern is to tie a pipeline's lifetime to another stream.

### `scope` — terminate when any notifier terminates

`scope` takes one or more notifier streams. The moment any of them terminates (for any reason — abort or complete), the main pipeline terminates too.

```typescript
const $unmount = new Signal<void>();

fromEventTarget(window, "resize").pipe(scope($unmount)).pipe(listen(handleResize));

// When the component unmounts:
$unmount.push(); // Signal auto-terminates after push, which terminates the resize pipeline
```

This is the idiomatic way to manage component lifetimes. Create a `Signal`, pass it to `scope`, push when you want everything to clean up.

### `scopeStrict` — terminate only when ALL notifiers have terminated

`scopeStrict` is the opposite policy. The main pipeline keeps running until every single notifier has terminated. Think of it as a countdown:

```typescript
const $task1Done = fromTimeout(1000);
const $task2Done = fromTimeout(3000);

// Keep polling until both background tasks are done
fromInterval(500).pipe(scopeStrict($task1Done, $task2Done)).pipe(listen(checkProgress));
// runs for 3 seconds — stops only when the last notifier (task2Done$) fires
```

---

## `debounce` vs `pace` — Two Different Rate-Limiting Strategies

Both operators slow down a stream, but they solve different problems.

### `debounce` — wait for silence

`debounce` suppresses all values until the source goes quiet for a specified duration. Every new value resets the timer. Only the last value in a burst gets through.

```typescript
// User is typing — only react after they stop for 300ms
fromEventTarget(input, "input").pipe(debounce(300)).pipe(listen(search));

// If user types 5 characters quickly, only the last one triggers search
```

Use `debounce` when you want to react to the _end_ of a burst of activity.

### `pace` — enforce a minimum gap between emissions

`pace` ensures at least `ms` milliseconds pass between each emitted value. Unlike `debounce`, it doesn't suppress values — it schedules them. The first value always fires immediately. If the next value arrives before the interval has elapsed, it waits. If it arrives _after_ the interval has already elapsed, it fires immediately — no artificial delay is added.

This is what distinguishes `pace` from `delay`. `delay` always adds a fixed offset to every value, regardless of when it arrived. `pace` only delays when the source is faster than the interval — if the source is already slow enough, values pass through without any delay at all.

```typescript
// A sensor fires 100 times per second — only process 2 per second
fromEventTarget(sensor, "data").pipe(pace(500)).pipe(listen(process));
```

Use `pace` when you want to _spread out_ a fast stream without losing values, and without adding unnecessary latency when the source is already slow.

---

## The `EMPTY` Sentinel

The library uses a special symbol called `EMPTY` (defined as `Symbol.for('empty')`) to represent the absence of a value. You'll encounter it in two places.

First, in `combine`: when an input hasn't emitted yet, its slot in the tuple is `EMPTY` rather than `undefined`. This is intentional — `undefined` is a valid value that a stream might emit. `EMPTY` is unambiguous.

Second, in `first` and `last`: if the stream completes without emitting any values, or if it aborts, these operators emit `EMPTY` rather than nothing. This means your downstream handler always gets called — you just check whether the value is `EMPTY`:

```typescript
of() // empty stream
  .pipe(first())
  .pipe(
    listen((v) => {
      if (v === EMPTY) console.log("nothing came through");
      else console.log("got:", v);
    }),
  );
```

Similarly, `last` emits `EMPTY` on abort (not on complete with no values — that also emits `EMPTY`, but the distinction matters: abort means the stream was cut short, complete means it finished naturally with nothing to emit).

---

## `every` — Short-Circuits on First Failure

`every` checks whether all values satisfy a predicate. What makes it interesting is that it terminates early the moment a value fails — it doesn't wait for the stream to complete:

```typescript
of(1, -1, 3, 4)
  .pipe(every((v) => v > 0))
  .pipe(listen(console.log)); // false — terminates after seeing -1, never processes 3 or 4
```

For an empty stream, `every` emits `true` — vacuous truth, consistent with how `Array.every` behaves.

---

## `range` Transformer — Slice by Index

Not to be confused with `fromRange` (which generates integers), the `range` _transformer_ slices a stream by index position. It takes a `start` index and an `offset` (count), skipping values before `start` and terminating after `offset` values:

```typescript
of("a", "b", "c", "d", "e").pipe(range(1, 3)).pipe(listen(console.log)); // 'b', 'c', 'd'
```

This is essentially `skip(start).pipe(take(offset))` but as a single operator.

---

## `scanArray` — Collect Into a Growing Array

`scanArray` is a specialized version of `scan` that accumulates all values into a single array, emitting the same array reference (mutated in place) after each push. Use it with `last()` to collect a stream into an array:

```typescript
of(1, 2, 3).pipe(scanArray()).pipe(last()).pipe(listen(console.log)); // [1, 2, 3]
```

Because it mutates and re-emits the same array reference, it's more memory-efficient than creating a new array on each step. The tradeoff: if you hold a reference to an intermediate emission, it will reflect later mutations. Use `last()` to get the final result and avoid that.

---

## `terminate` Transformer — Observe the End

`terminate` ignores all values and emits only the termination reason — either `'complete'` or `'abort'` — when the stream ends. It's useful when you care about _when_ something finishes, not _what_ it emitted:

```typescript
$fetch.pipe(terminate()).pipe(
  listen((reason) => {
    if (reason === "complete") cleanup();
    if (reason === "abort") rollback();
  }),
);
```

Compare this to `tapTerminate`, which runs a side effect on termination but passes values through unchanged. `terminate` replaces the value stream entirely with a single termination event.

### Value-changing transformers break the pipeline — by design

Some transformers fundamentally change the type of what flows through the pipeline. `terminate` discards all values and emits a termination reason. `last` waits for completion and emits a single value. `every` short-circuits and emits a boolean. Once you pipe through any of these, the downstream type is different — you can't continue the original pipeline after them.

This is intentional. These transformers are meant to be used on a **detached branch**, not inline in the main pipeline. There are two clean ways to do that.

The first is to split the pipeline at an intermediate stage and attach the side branch from there:

```typescript
const $results = $source.pipe(map(transform)).pipe(resolve());

// main pipeline continues
$results.pipe(listen(render));

// side branch — detached, doesn't affect the main pipeline
$results.pipe(terminate()).pipe(listen((reason) => cleanup(reason)));
$results.pipe(last()).pipe(listen((v) => log("final value:", v)));
```

The second is `tapInput`, which gives you access to the next transformer in the chain _before_ it's attached, letting you branch off without breaking the fluent chain or introducing intermediate variables:

```typescript
source
  .pipe(map(transform))
  .pipe(resolve())
  .pipe(
    tapInput(($r) => {
      $r.pipe(terminate()).pipe(listen((reason) => cleanup(reason)));
      $r.pipe(last()).pipe(listen((v) => log("final value:", v)));
    }),
  )
  .pipe(listen(render)); // main pipeline continues unaffected
```

Both approaches are equivalent. The split-variable style is clearer when the side branch is substantial. `tapInput` is cleaner when you want to keep everything in one fluent expression.

---

## Putting It Together

Here's a real-world pipeline: a search input that debounces, fetches with concurrency control, and shares results across multiple consumers — one for rendering, one for logging.

```typescript
const $results = fromEventTarget(searchInput, "input")
  .pipe(map((e) => (e.target as HTMLInputElement).value))
  .pipe(debounce(300))
  .pipe(map((query) => fetch(`/api/search?q=${query}`)))
  .pipe(resolve(1))
  .pipe(share());

// Renders results
$results.pipe(listen(renderResults));

// Logs without driving the fetch
$results.pipe(passive()).pipe(listen((v) => analytics.track("search_result")));
```

Every piece of behavior is a transformer. The core never knew about debouncing, promises, sharing, or passive observation. It just moved values from producers to consumers at the rate each consumer requested.

---

## The Principle That Survived Everything

After more than three hundred iterations from scratch and from first principles, one principle survived every redesign:

> **Primitives must be dumb and unbounded. Behavior lives in higher layer.**

Every time I violated this — making the queue bounded by default, making the core error-aware, making the protocol promise-aware — complexity multiplied and the hot path suffered.

Every time I respected it — moving error handling to `safe`, moving queue bounds to `latest` and `batch`, moving async to `resolve` — the core got simpler and the system got more composable.

The three primitives (`stream/source/consumable`, `pipe`, `transformers`) can express anything. The rest is just transformers all the way down.

---

## The Protocol Is Not JavaScript

Everything described in this article — the credit counter, the handler registered once at construction, the reentrancy guard, the transducer pattern, the separation of core from behavior — none of it is specific to JavaScript.

The pull-on-push protocol is a design, not an implementation. The core ideas map directly to any language with first-class functions and mutable state:

- A **consumer** is a struct or object with a credit counter, a queue, and a handler.
- **`next()`** increments credit and drains the queue.
- **`push()`** fires immediately if credit is available, otherwise enqueues.
- A **transformer** is any type that holds a reference to an upstream consumable and wraps the handler on `consume`.
- **`pipe`** is function application.

Rust, Go, Swift, Kotlin, C# — the protocol fits all of them. The reentrancy guard is a single integer comparison. The transducer pattern is just nested closures or lambdas. The separation of core from behavior is a layering decision, not a language feature.

The TypeScript implementation here is one expression of the protocol. The protocol itself is the idea worth keeping.

---

## 🗺️ Roadmap

- **Ecosystem Branding & Visual Identity** – Establish distinct brand guidelines, a dedicated documentation site, and visual asset packages to support community adoption and sponsor visibility.

- **Cross-Language Protocol Implementations** – Port the core "Pull-on-Push" credit specification to other mainstream programming languages to enable native, cross-platform distributed streaming:
  - `Rust`
  - `Zig`
  - `Go`
  - `Python`
  - `Swift`
  - `Kotlin`
  - `C#`
  - `Java`
- **Core** - adding more sources,streams and transformers.

- **Multithreaded Transformers (`Web-Workers` / `Worker-Threads` Integration)** – Develop native, worker-driven parallel execution pipelines. While standard 3rd-party libraries already pair seamlessly with our standard `map` + `resolve` primitives, native multithreaded handlers will unlock zero-copy data processing for heavy workloads .

- **Domain-Specific Transformer Packages** – Expand the core protocol into a modular ecosystem under the `@watchmanlab` scope:
  - `@watchmanlab/util` – A suite of common helper and utility transformers.
  - `@watchmanlab/io` – Streamlined asynchronous file system I/O primitives.
  - `@watchmanlab/dom` – High-performance, backpressure-aware DOM event handlers.
  - `@watchmanlab/http` & `@watchmanlab/websocket` – Servers and Network-layer streaming for standard web protocols.
  - `@watchmanlab/rpc` – Low-overhead Remote Procedure Call stream boundaries.
  - `@watchmanlab/pubsub` – A masterless, highly available Pub/Sub architecture supporting agnostic transport protocols (HTTP, TCP, UDP, and Unix Domain Sockets).

_`@watchmanlab/stream` is available on npm and JSR. TypeScript 5.x, zero runtime dependencies, ESM only._
