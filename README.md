# @soffinal/stream

> Multi-paradigm reactive primitives for modern applications

A high-performance, pull-based reactive stream library for TypeScript. Built around a credit-based backpressure model, it gives you fine-grained control over data flow with zero implicit buffering.

---

## Table of Contents

- [Installation](#installation)
- [Core Concepts](#core-concepts)
  - [Consumer](#consumer)
  - [Stream](#stream)
  - [Source](#source)
  - [Consumable](#consumable)
- [Sources](#sources)
- [Transformers](#transformers)
- [Special Streams](#special-streams)
- [Performance](#performance)

---

## Installation

```bash
npm install @soffinal/stream
# or
bun add @soffinal/stream
```

---

## Core Concepts

### Consumer

The fundamental unit of consumption. A `Consumer<VALUE>` receives values one at a time and controls its own flow via a **credit system** — it only processes a value when it has called `next()` to signal readiness.

```ts
const consumer = new Consumer<number>((self, value) => {
  console.log(value);
  self.next(); // request the next value
});

consumer.next(); // grant initial credit
consumer.push(1);
consumer.push(2);
consumer.push(3);
```

**Key methods:**

| Method                | Description                                                                |
| --------------------- | -------------------------------------------------------------------------- |
| `push(value)`         | Deliver a value. Queued if no credit available.                            |
| `next()`              | Grant one credit, dequeue and process if values are waiting.               |
| `terminate(reason)`   | End the consumer. `"abort"` drops the queue, `"complete"` drains it first. |
| `pushBatch(values[])` | Push multiple values at once.                                              |

**Lifecycle events (as `Source<T>`):**

| Property     | Emits                                   |
| ------------ | --------------------------------------- |
| `$push`      | Every value pushed (queued or handled)  |
| `$handle`    | Every value actually handled            |
| `$next`      | Every `next()` call when queue is empty |
| `$enqueue`   | Every value added to the queue          |
| `$dequeue`   | Every value dequeued                    |
| `$drain`     | When consumer enters drain state        |
| `$terminate` | When consumer terminates                |

**Options:**

```ts
new Consumer(handler, {
  queueFactory: () => new DefaultQueue(), // custom queue
  init: (consumer) => cleanupFn, // setup + optional cleanup
  push,
  next,
  drain,
  enqueue,
  dequeue,
  terminate, // lifecycle hooks
});
```

---

### Stream

A `Stream<VALUE>` is a multicast push source. It holds a set of consumers and broadcasts every pushed value to all of them. It extends `Source` and implements `Disposable`.

```ts
const stream = new Stream<number>();

stream
  .consume((self, value) => {
    console.log(value);
    self.next();
  })
  .next();

stream.push(1).push(2).push(3);
stream.terminate("complete");
```

**Key methods:**

| Method                       | Description                            |
| ---------------------------- | -------------------------------------- |
| `push(value)`                | Broadcast value to all consumers       |
| `consume(handler, options?)` | Subscribe a new consumer               |
| `terminate(reason)`          | Terminate the stream and all consumers |
| `pushBatch(values[])`        | Push multiple values                   |

**Lifecycle events (as `Source<T>`):**

| Property             | Emits                          |
| -------------------- | ------------------------------ |
| `$push`              | Every pushed value             |
| `$next`              | Every consumer `next()` call   |
| `$consumerJoin`      | Every new consumer             |
| `$consumerLeft`      | Every consumer that leaves     |
| `$firstConsumerJoin` | When the first consumer joins  |
| `$lastConsumerLeft`  | When the last consumer leaves  |
| `$drain`             | When stream enters drain state |
| `$terminate`         | When stream terminates         |

**Static factory:**

```ts
// Wrap any Consumable into a Stream (unicast → multicast bridge)
const stream = Stream.from(someConsumable);
```

**Options:**

```ts
new Stream({
  consumerSetFactory: () => new DefaultConsumerSet(),
  init,
  push,
  next,
  consumerJoin,
  consumerLeft,
  firstConsumerJoin,
  lastConsumerLeft,
  drain,
  terminate,
});
```

---

### Source

Abstract base class for all stream sources and transformers. Provides the `pipe` method for composing transformers and `Symbol.asyncIterator` for async iteration.

```ts
abstract class Source<VALUE> {
  pipe<OUTPUT>(transform: ($input: this) => OUTPUT): OUTPUT;
  abstract consume(handler, options?): Consumer<VALUE>;
  static from(consumable): Source<VALUE>;
}
```

**Async iteration:**

```ts
const stream = fromIterable([1, 2, 3]);
for await (const value of stream) {
  console.log(value);
}
```

---

### Consumable

Interface implemented by both `Stream` and all transformer classes. Anything with a `consume(handler, options?)` method is a `Consumable`.

```ts
interface Consumable<VALUE> {
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
}
```

---

## Sources

Factory functions that create streams from external data.

### `of(...values)`

Emits a fixed list of values then completes.

```ts
of(1, 2, 3).pipe(listen(console.log));
```

---

### `fromIterable(iterable)`

Wraps any `Iterable` (arrays, sets, strings, etc.).

```ts
fromIterable([1, 2, 3]).pipe(listen(console.log));
fromIterable(() => new Set([1, 2, 3])); // factory form
```

---

### `fromIterator(iterator)`

Wraps an existing `Iterator`.

```ts
fromIterator([1, 2, 3].values()).pipe(listen(console.log));
```

---

### `fromGenerator(generatorFn)`

Wraps a generator function. A new generator is created per consumer.

```ts
fromGenerator(function* () {
  yield 1;
  yield 2;
  yield 3;
}).pipe(listen(console.log));
```

---

### `fromAsyncIterable(asyncIterable)`

Wraps an `AsyncIterable`.

```ts
fromAsyncIterable(asyncIterable).pipe(listen(console.log));
```

---

### `fromAsyncIterator(asyncIterator)`

Wraps an `AsyncIterator`.

```ts
fromAsyncIterator(asyncIterator).pipe(listen(console.log));
```

---

### `fromAsyncGenerator(asyncGeneratorFn)`

Wraps an async generator function.

```ts
fromAsyncGenerator(async function* () {
  yield await fetch("/api/a").then((r) => r.json());
  yield await fetch("/api/b").then((r) => r.json());
}).pipe(listen(console.log));
```

---

### `fromInterval(ms)`

Emits an incrementing counter every `ms` milliseconds. Never completes on its own.

```ts
fromInterval(500).pipe(listen(console.log)); // 0, 1, 2, ...
```

---

### `fromTimeout(ms, value?)`

Emits a single value after `ms` milliseconds then completes.

```ts
fromTimeout(1000, "done").pipe(listen(console.log));
```

---

### `fromRange(start, end)`

Emits integers from `start` (inclusive) to `end` (exclusive).

```ts
fromRange(1, 5).pipe(listen(console.log)); // 1, 2, 3, 4
```

---

### `fromFunction(fn)`

Calls `fn` once per `next()` and emits the return value, then completes.

```ts
fromFunction(() => Math.random()).pipe(listen(console.log));
```

---

### `fromEventTarget(target, eventType)`

Emits DOM events of the given type from an `EventTarget`.

```ts
fromEventTarget(window, "click").pipe(listen((e) => console.log(e.type)));
```

---

### `fromAbortSignal(signal)`

Emits once when the `AbortSignal` is aborted, then completes.

```ts
const controller = new AbortController();
fromAbortSignal(controller.signal).pipe(listen(() => console.log("aborted")));
controller.abort();
```

---

### `fromAbortController(controller)`

Convenience wrapper around `fromAbortSignal`.

```ts
fromAbortController(controller).pipe(listen(() => console.log("aborted")));
```

---

### `fromGCToken(token)`

Emits once when the given object is garbage collected (via `FinalizationRegistry`). Useful for lifecycle-bound cleanup.

```ts
let obj = {};
fromGCToken(obj).pipe(listen(() => console.log("collected")));
obj = null; // allow GC
```

---

## Transformers

All transformers are used via `.pipe()`. Each returns a new `Source`.

### `map(mapper)`

Transforms each value.

```ts
of(1, 2, 3)
  .pipe(map((v) => v * 2))
  .pipe(listen(console.log)); // 2, 4, 6
```

---

### `filter(predicate)`

Passes only values matching the predicate. Non-matching values are forwarded to `$complements`.

```ts
const filtered = of(1, 2, 3, 4).pipe(filter((v) => v % 2 === 0));

filtered.$complements.pipe(listen(console.log)); // 1, 3
filtered.pipe(listen(console.log)); // 2, 4
```

---

### `tap(callback)`

Side-effect per value, passes values through unchanged.

```ts
of(1, 2, 3)
  .pipe(tap((v) => console.log("tap", v)))
  .pipe(listen());
```

---

### `take(count)`

Takes the first `count` values then terminates.

```ts
fromInterval(100).pipe(take(3)).pipe(listen(console.log)); // 0, 1, 2
```

---

### `takeWhile(predicate)`

Takes values while predicate is true, terminates on first false.

```ts
of(1, 2, 3, 4)
  .pipe(takeWhile((v) => v < 3))
  .pipe(listen(console.log)); // 1, 2
```

---

### `takeUntil(notifier)`

Takes values until the notifier emits.

```ts
const stop = fromTimeout(1000);
fromInterval(200).pipe(takeUntil(stop)).pipe(listen(console.log));
```

---

### `skip(count)`

Skips the first `count` values.

```ts
of(1, 2, 3, 4).pipe(skip(2)).pipe(listen(console.log)); // 3, 4
```

---

### `skipWhile(predicate)`

Skips values while predicate is true.

```ts
of(1, 2, 3, 4)
  .pipe(skipWhile((v) => v < 3))
  .pipe(listen(console.log)); // 3, 4
```

---

### `skipUntil(notifier)`

Skips values until the notifier emits.

```ts
const start = fromTimeout(600);
stream.pipe(skipUntil(start)).pipe(listen(console.log));
```

---

### `reduce(initialAcc, reducer)`

Accumulates all values and emits the final result on completion.

```ts
of(1, 2, 3, 4)
  .pipe(reduce(0, (acc, v) => acc + v))
  .pipe(listen(console.log)); // 10
```

---

### `scan(initialAcc, reducer)`

Like `reduce` but emits the running accumulator after each value.

```ts
of(1, 2, 3)
  .pipe(scan(0, (acc, v) => acc + v))
  .pipe(listen(console.log)); // 1, 3, 6
```

---

### `distinct(keySelector?, flushes?)`

Filters out duplicate values. Optionally use a key selector and a flush notifier to reset seen keys.

```ts
of(1, 1, 2, 2, 3).pipe(distinct()).pipe(listen(console.log)); // 1, 2, 3

of({ id: 1 }, { id: 1 }, { id: 2 })
  .pipe(distinct((v) => v.id))
  .pipe(listen(console.log));
```

---

### `find(predicate)`

Emits `{ ok: true, value }` for the first matching value, or `{ ok: false, error: "not-found" }` on completion.

```ts
of(1, 2, 3, 4)
  .pipe(find((v) => v > 2))
  .pipe(unwrap())
  .pipe(listen(console.log)); // 3
```

---

### `first()`

Emits `{ ok: true, value }` for the first value, or `{ ok: false, error: "not-found" }`.

```ts
of(1, 2, 3).pipe(first()).pipe(unwrap()).pipe(listen(console.log)); // 1
```

---

### `last()`

Emits `{ ok: true, value }` for the last value on completion, or `{ ok: false, error: "not-found" }`.

```ts
of(1, 2, 3).pipe(last()).pipe(unwrap()).pipe(listen(console.log)); // 3
```

---

### `unwrap(errorHandler?)`

Unwraps `Result<VALUE, ERROR>` — passes `value` downstream on success, calls `errorHandler` on failure.

```ts
of(1, 2, 3)
  .pipe(first())
  .pipe(unwrap((err) => console.error(err)))
  .pipe(listen(console.log));
```

---

### `result(fn)`

Wraps a transformer pipeline in a try/catch, emitting `{ ok: true, value }` or `{ ok: false, error }`.

```ts
of(1, 2, 3)
  .pipe(
    result(
      map((v) => {
        if (v === 2) throw new Error("!");
        return v;
      }),
    ),
  )
  .pipe(listen(console.log));
```

---

### `merge(...others)`

Merges multiple streams into one. Emits values from all inputs as they arrive.

```ts
const s1 = fromInterval(500).pipe(map(() => "A"));
const s2 = fromInterval(700).pipe(map(() => "B"));
s1.pipe(merge(s2)).pipe(listen(console.log));
```

---

### `merge$(concurrent?, depth?)`

Flattens a stream of `Consumable`s by subscribing to each inner stream concurrently up to `concurrent` limit.

```ts
of(of(1, 2), of(3, 4)).pipe(merge$()).pipe(listen(console.log)); // 1, 2, 3, 4
```

---

### `zip(...others)`

Combines values from multiple streams pairwise. Emits a tuple when all inputs have a new value. Remaining unmatched values are available on `$rest`.

```ts
const s1 = new Stream<number>();
const s2 = new Stream<string>();

const zipped = s1.pipe(zip(s2));
zipped.pipe(listen(console.log)); // [1, "a"], [2, "b"]

s1.push(1);
s2.push("a");
s1.push(2);
s2.push("b");
```

---

### `combine(...others)`

Emits the latest value from each input whenever any input emits. Unlike `zip`, does not wait for all inputs to have new values.

```ts
const s1 = of(1, 2, 3).pipe(delay(100));
const s2 = of("a", "b").pipe(delay(200));
s1.pipe(combine(s2)).pipe(listen(console.log));
```

---

### `flat(depth?)`

Flattens a stream of arrays. `depth` controls how deep to flatten (default `0` = one level).

```ts
of([1, 2], [3, 4]).pipe(flat()).pipe(listen(console.log)); // 1, 2, 3, 4
```

---

### `flat$(depth?)`

Flattens a stream of `Consumable`s sequentially (one at a time).

```ts
of(of(1, 2), of(3, 4)).pipe(flat$()).pipe(listen(console.log)); // 1, 2, 3, 4
```

---

### `switch$()`

Subscribes to the latest inner `Consumable`, cancelling the previous one when a new one arrives.

```ts
of(streamA, streamB, streamC).pipe(switch$()).pipe(listen(console.log));
```

---

### `resolve(concurrency?)`

Resolves a stream of `Promise`s with optional concurrency control. Emits `Result<VALUE, ERROR>`.

```ts
of(fetch("/a"), fetch("/b")).pipe(resolve(2)).pipe(unwrap()).pipe(listen(console.log));
```

---

### `delay(ms)`

Delays each value by `ms` milliseconds.

```ts
of(1, 2, 3).pipe(delay(500)).pipe(listen(console.log));
```

---

### `delayOnce(ms)`

Delays only the first value, subsequent values pass through immediately.

```ts
of(1, 2, 3).pipe(delayOnce(500)).pipe(listen(console.log));
```

---

### `debounce(ms)`

Emits a value only after `ms` milliseconds of silence (no new values).

```ts
stream.pipe(debounce(300)).pipe(listen(console.log));
```

---

### `pace(ms)`

Rate-limits output to at most one value per `ms` milliseconds.

```ts
fromInterval(50).pipe(pace(500)).pipe(listen(console.log));
```

---

### `tick()`

Defers each value to the next microtask via `queueMicrotask`.

```ts
of(1, 2, 3).pipe(tick()).pipe(listen(console.log));
```

---

### `buffer(size, startBufferEvery?)`

Collects values into fixed-size arrays. `startBufferEvery` controls sliding window overlap.

```ts
of(1, 2, 3, 4, 5).pipe(buffer(2)).pipe(listen(console.log));
// [1,2], [3,4], [5]
```

---

### `batch(size)`

Collects values into arrays of up to `size`. Emits remaining values on completion.

```ts
of(1, 2, 3, 4, 5).pipe(batch(2)).pipe(listen(console.log));
// [1,2], [3,4], [5]
```

---

### `toArray()`

Collects all values into a single array, emitted on completion.

```ts
of(1, 2, 3).pipe(toArray()).pipe(listen(console.log)); // [1, 2, 3]
```

---

### `range(start, offset)`

Passes only values at indices `[start, start + offset)`.

```ts
of("a", "b", "c", "d", "e").pipe(range(1, 3)).pipe(listen(console.log)); // b, c, d
```

---

### `index()`

Replaces each value with its zero-based index.

```ts
of("a", "b", "c").pipe(index()).pipe(listen(console.log)); // 0, 1, 2
```

---

### `context(ctx)`

Wraps each value with a shared context object: `{ value, context }`.

```ts
of(1, 2, 3)
  .pipe(context({ count: 0 }))
  .pipe(tap((v) => v.context.count++))
  .pipe(listen((v) => console.log(v.value, v.context.count)));
```

---

### `scope(...notifiers)`

Terminates the stream when any of the notifier streams emit.

```ts
const lifetime = fromTimeout(5000);
fromInterval(500).pipe(scope(lifetime)).pipe(listen(console.log));
```

---

### `gate(control)`

Opens or closes the stream based on a boolean control stream. `true` opens, `false` closes.

```ts
const toggle = fromInterval(3000).pipe(map((_, i) => i % 2 === 0));
fromInterval(500).pipe(gate(toggle)).pipe(listen(console.log));
```

---

### `share()`

Converts a `Consumable` into a multicast `Stream` via `Stream.from`.

```ts
const shared = fromInterval(500).pipe(share());
shared.pipe(listen((v) => console.log("A", v)));
shared.pipe(listen((v) => console.log("B", v)));
```

---

### `replay(values)`

Replays a fixed list of values to each new consumer before forwarding live values.

```ts
stream.pipe(replay([1, 2, 3])).pipe(listen(console.log));
```

---

### `replayLatest(count)`

Replays the last `count` values to each new consumer.

```ts
const hot = fromInterval(200).pipe(share()).pipe(replayLatest(3));
setTimeout(() => hot.pipe(listen(console.log)), 2000); // gets last 3 values immediately
```

---

### `latests(count)`

Like `replayLatest` but buffers values passively before any consumer subscribes.

```ts
const buffered = fromInterval(300).pipe(latests(2));
setTimeout(() => buffered.pipe(listen(console.log)), 2000);
```

---

### `passive()`

Subscribes to the upstream without requesting values via `next()`. Values are only received when the upstream pushes them independently.

```ts
source.pipe(passive()).pipe(listen(console.log));
```

---

### `pump()`

Eagerly drains the upstream into an internal `Stream`, decoupling producer and consumer timing.

```ts
const pumped = expensiveSource.pipe(pump());
pumped.pipe(listen(console.log));
```

---

### `listen(callback?)`

Terminal operator — subscribes and calls `next()` automatically. Returns the input source for further chaining.

```ts
of(1, 2, 3).pipe(listen(console.log));
```

---

### `print(label?)`

Logs each value to the console (with optional label) and passes it through.

```ts
of(1, 2, 3).pipe(print("value:")).pipe(listen());
```

---

### `toConsole()`

Shorthand for `.pipe(listen(console.log))`.

```ts
of(1, 2, 3).pipe(toConsole());
```

---

### `tapBatch(callback)`

Side-effect for each item inside a batch (stream of arrays), passes the array through unchanged.

```ts
of([1, 2], [3])
  .pipe(tapBatch((v) => console.log("item", v)))
  .pipe(listen());
```

---

### `mapBatch(mapper)`

Maps each item inside a batch (stream of arrays).

```ts
of([1, 2], [3])
  .pipe(mapBatch((v) => v * 2))
  .pipe(listen(console.log));
// [2, 4], [6]
```

---

## Special Streams

### `Signal<VALUE>`

A `Stream` that automatically terminates after the first push. Useful for one-shot events.

```ts
import { Signal } from "@soffinal/stream/streams/signal";

const signal = new Signal<string>();
signal.pipe(listen(console.log));
signal.push("done"); // emits "done" then terminates
```

---

### `State<VALUE>`

A `Stream` with a readable/writable `.value` property. Setting `.value` pushes to all consumers.

```ts
import { state } from "@soffinal/stream/streams/state";

const count = state(0);
count.pipe(listen((v) => console.log("count:", v)));
count.value = 1; // logs "count: 1"
count.value = 2; // logs "count: 2"
```

---

## Performance

Benchmarks run with Bun on a 300-stage synchronous pipeline:

```
=========================================
⚡ SYNC MEGA-PIPELINE ENGINE BENCHMARK ⚡
=========================================
Streaming 1,000,000 values through 300 stages...

Total Time        : ~850 ms
Total Stage Pushes: 300,000,000 operations
Engine Velocity   : ~350,000,000 ops/sec
=========================================
```

Memory profile (5,000 pipelines × 200 stages):

```
Total Heap Increase:   ~140 MB
Average Per Pipeline:  ~29,546 bytes
Average Per Stage:     ~148 bytes
```

Compared to RxJS (same workload):

- **~3× faster** throughput
- **~33% less** memory per stage (~148 bytes vs ~222 bytes)

---

## License

MIT © [Soffinal](https://github.com/soffinal/stream)
