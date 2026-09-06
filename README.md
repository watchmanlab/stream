# @soffinal/stream

> Multi-paradigm reactive primitives built on a **pull-on-push** protocol.

A TypeScript reactive library with a credit-based backpressure system, zero runtime dependencies, and a composable transformer API. Every behavior lives in a transformer — the core stays dumb.

---

## Install

```sh
# npm
npm install @soffinal/stream

# JSR
jsr add @soffinal/stream
```

---

## Core Concepts

### The Three Primitives

Everything is built from three concepts:

- **`Stream` / `Source` / `Consumable`** — one primitive, three layers. `Consumable` is the interface: anything with a `consume` method participates in the pipeline. `Source` is the abstract base that adds `pipe` and `for await...of`. `Stream` is the concrete multicast implementation. From the user's perspective they are interchangeable.
- **`pipe`** — the composition mechanism. `source.pipe(fn)` is just `fn(source)`. Transformers are not special — they are plain `Source`s whose constructor accepts another `Consumable` as input. `map`, `filter`, `debounce` are all just `Source` subclasses. `pipe` is the glue that passes one into the next.

### The Credit System

A `Consumer` only processes a value when it has **credit**. Credit is granted by calling `next()`. Values pushed without credit are queued and drained in order as credit is granted.

```typescript
const consumer = new Consumer<number>((self, value) => {
  console.log(value);
  self.next(); // grant credit for the next value
});
consumer.next(); // grant initial credit
consumer.push(1); // fires immediately
consumer.push(2); // fires immediately (self.next() re-granted credit)
```

A synchronous consumer always has credit before values arrive — the queue is never touched. An async consumer buffers naturally. Backpressure is implicit and free.

---

## Quick Start

```typescript
import { of, fromInterval, fromIterable } from "@soffinal/stream/sources";
import { map, filter, take, listen, share, debounce, resolve } from "@soffinal/stream/transformers";
import { Stream, Signal, State } from "@soffinal/stream";

// Basic pipeline
of(1, 2, 3, 4, 5)
  .pipe(filter((v) => v % 2 === 0))
  .pipe(map((v) => v * 10))
  .pipe(listen(console.log)); // 20, 40

// Interval with take
fromInterval(100).pipe(take(3)).pipe(listen(console.log)); // 0, 1, 2

// for await...of
for await (const v of fromIterable([1, 2, 3])) {
  console.log(v);
}
```

---

## Sources

| Factory                           | Description                                       |
| --------------------------------- | ------------------------------------------------- |
| `of(...values)`                   | Fixed list of values, then completes              |
| `fromIterable(iterable)`          | Any `Iterable` or factory function                |
| `fromGenerator(fn)`               | Generator function, new instance per consumer     |
| `fromInterval(ms)`                | `setInterval` counter, never completes on its own |
| `fromTimeout(ms, value?)`         | `setTimeout`, single value then completes         |
| `fromRange(start, end)`           | Integer range `[start, end)`                      |
| `fromEventTarget(target, type)`   | DOM `EventTarget` events                          |
| `fromAbortSignal(signal)`         | Emits once when `AbortSignal` fires               |
| `fromFunction(fn)`                | Calls `fn` once, emits result then completes      |
| `fromAsyncIterable(iterable)`     | Any `AsyncIterable`                               |
| `fromAsyncIterator(iterator)`     | Any `AsyncIterator`                               |
| `fromAsyncGenerator(fn)`          | Async generator function                          |
| `fromAbortController(controller)` | Emits once when controller is aborted             |
| `fromGcToken(target)`             | Emits when a `WeakRef` target is GC'd             |

Sources marked **replayable** create a fresh sequence per consumer.

---

## Streams

### `Stream<VALUE>`

Multicast push source with observable lifecycle events.

```typescript
const stream = new Stream<number>();

// Lifecycle as first-class sources
stream.$push.pipe(listen((v) => console.log("pushed:", v)));
stream.$firstConsumerJoin.pipe(listen(() => console.log("first consumer")));
stream.$terminate.pipe(listen((r) => console.log("terminated:", r)));

stream
  .consume((c, v) => {
    console.log(v);
    c.next();
  })
  .next();
stream.push(1).push(2).push(3);
stream.terminate("complete");
```

**Lifecycle events:** `$push`, `$next`, `$consumerJoin`, `$consumerLeft`, `$firstConsumerJoin`, `$lastConsumerLeft`, `$drain`, `$terminate`

### `Signal<VALUE>`

Auto-terminates after the first push. For one-shot events.

```typescript
const sig = new Signal<string>();
sig.pipe(listen(console.log));
sig.push("done"); // emits 'done', then terminates
```

### `State<VALUE>`

Has a `.value` property. Setting it pushes to all consumers.

```typescript
const count = state(0);
count.pipe(listen((v) => console.log("count:", v)));
count.value = 1; // logs 'count: 1'
count.value = 2; // logs 'count: 2'
```

---

## Transformers

### Filtering & Slicing

```typescript
filter(predicate)       // pass values matching predicate; rejected values go to .$complements
take(n)                 // first n values then terminate
skip(n)                 // skip first n values
takeWhile(predicate)    // take while predicate is true
takeUntil(notifier)     // take until notifier emits
skipWhile(predicate)    // skip while predicate is true
skipUntil(notifier)     // skip until notifier emits
first(predicate?)       // first value (or first matching predicate)
last()                  // last value on completion
distinct(comparator?)   // skip consecutive duplicates
```

### Transformation

```typescript
map(fn); // transform each value
tap(fn); // side effect, passes value through
tapInput(fn); // tap on the input consumer
tapTerminate(fn); // tap on termination
scan(seed, reducer); // running accumulator, emits each step
scanArray(fn); // scan that accumulates into an array
context(ctx); // wrap each value as { value, context }
```

### Flattening

```typescript
flat(depth?)            // flatten arrays
flat$()                 // flatten inner Consumables sequentially
switch$()               // switch to latest inner Consumable, cancel previous
```

### Combination

```typescript
merge(...others); // emit from all inputs as they arrive
zip(...others); // pair values from all inputs into tuples
combine(...others); // emit on any input, using last known value for others
```

### Timing

```typescript
debounce(ms); // emit after ms silence
pace(ms); // rate-limit to one value per ms
delay(ms); // delay each value by ms
delayOnce(ms); // delay only the first value
```

### Windowing

```typescript
buffer(size, every?)    // sliding window arrays
batch(size)             // non-overlapping chunks
```

### Sharing & Routing

```typescript
share(); // multicast via Stream.from
passive(); // observe without driving the source
gate(control$); // open/close based on boolean stream
pump(); // eagerly drain upstream into a Stream
latest(n); // buffer last n values for late subscribers
```

### Async

```typescript
resolve(concurrency?)   // resolve Promises, emit values or Error<reason>
```

### Error Handling

```typescript
safe(fn); // wrap pipeline in try/catch, emit Error<thrown> on throw
```

### Lifecycle

```typescript
terminate(); // emit termination reason, ignore values
scope(...notifiers); // terminate when any notifier terminates
scopeStrict(...notifiers); // terminate only on abort
```

### Aggregation

```typescript
sum(); // running sum
min(); // running minimum
max(); // running maximum
count(); // running count
every(predicate); // true if all values match
range(); // [min, max] running range
```

> All aggregation operators emit running values. Pipe `last()` after them to get the final result.

```typescript
of(1, 2, 3, 4, 5)
  .pipe(scan(0, (acc, v) => acc + v))
  .pipe(last())
  .pipe(listen(console.log)); // 15
```

---

## Active vs Passive Consumers

By default every consumer is **active** — it drives the source. When a source is shared, the fastest consumer triggers production and slower ones buffer values in their queues.

Use `passive()` to observe a shared source without driving it:

```typescript
const shared = fromInterval(500).pipe(share());

shared.pipe(listen((v) => console.log("main:", v))); // drives the source
shared.pipe(passive()).pipe(listen((v) => console.log("observer:", v))); // never drives
```

---

## Error Handling

Errors are values. The core is error-agnostic. Use `safe` to catch throws and `resolve` to catch promise rejections:

```typescript
of(1, 2, 3)
  .pipe(
    safe(
      map((v) => {
        if (v === 2) throw "bad";
        return v;
      }),
    ),
  )
  .pipe(filter((v) => !(v instanceof Error)))
  .pipe(listen(console.log)); // 1, 3

fromGenerator(function* () {
  yield fetch("/a");
  yield fetch("/b");
})
  .pipe(resolve(2))
  .pipe(listen(console.log));
```

---

## Termination

Two modes:

- `"abort"` — stop immediately, clear all queues
- `"complete"` — drain remaining queued values, then stop

Works with `using` / `await using`:

```typescript
{
  using stream = new Stream<number>(); // abort on block exit
}

{
  await using stream = new Stream<number>(); // complete on block exit, waits for drain
}
```

---

## Lifecycle Example

```typescript
const stream = new Stream<number>({
  firstConsumerJoin: () => console.log("started"),
  lastConsumerLeft: () => console.log("stopped"),
});

const c = stream
  .consume((c, v) => {
    console.log(v);
    c.next();
  })
  .next();
stream.push(1).push(2);
c.terminate("abort"); // 'stopped'
```

---

## License

MIT
