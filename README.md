# WatchmanRx

One unified model across **sync & async**, **hot & cold**, **push & pull**, and **lazy & eager** execution.

## Install

```sh
npm install watchmanrx
```

## Contents

- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
  - [Consumable](#consumable)
  - [Consumer](#consumer)
  - [Source](#source)
  - [Stream](#stream)
- [Sources](#sources)
- [Streams](#streams)
- [Transformers](#transformers)
  - [Filtering](#filtering)
  - [Transformation](#transformation)
  - [Aggregation](#aggregation)
  - [Flow Control](#flow-control)
  - [Side Effects & Termination](#side-effects--termination)
- [Protocol](#protocol)

### Quick start

```ts
import { filter, map, iterableSource, listen, take, delay } from "watchmanrx";

// `source` is lazy and replayable
// so you can consume it multiple times
const source = iterableSource([1, 2, 3, 4, 5, 6])
  .pipe(filter((value) => value > 2))
  .pipe(map((value, index) => `${index}:${value}`))
  .pipe(take(3))
  .pipe(delay(100));

//consume it using:
for await (const value of source) {
  console.log(value);
}

//OR
source
  .consume((consumer, value) => {
    console.log(value);
    consumer.next();
  })
  .next();

//OR
source.pipe(listen((value) => console.log(value)));
```

## Core concepts

At first glance this looks like any other reactive/streaming library,but it's fundamentally different.

There is a `Consumable` and it need to be consumed by a one or more `Consumer`(s),this concept is well known but here we go deeper and explore many concepts about the relation between them.

Because this library is self referenced ,each primitive require the others primitives and even it self, we can't have a linear and progressive guidline, but when you use it all of that will make sense.

to put you in the context here is the five core primitives : `Consumable`,`Consumer`,`Source`, `Stream` and `Transformer`. -`Source` is a `Consumable` and have many implementations. -`Stream` and `Transformer` are `Source`.

Now we will dive deep into each one.

### Consumable

> [API Reference →](./API_REF.md#consumablevalue--interface)

The base contract of the library. Any object with a `consume` method is a `Consumable` and `consume` must return a `Consumer`.

### Consumer

> [API Reference →](./API_REF.md#consumervalue--class)

The fundamental unit of consumption. A `Consumer` receives values and controls its own processing rate through a **credit system**: a value is only processed when the consumer has credit, granted by calling its `.next()` method. Values pushed without available credit are buffered in an internal FIFO queue and drained in order as credit is granted.

> **Note:** queue is an interface, so you can provide any implementation (e.g RingBufferQueue, ObjectPoolQueue,SizedQueue,TTLQueue,...etc) this library provide a default one and its an unbounded linked list queue.

Generally you will not create a `Consumer` manually since you get it from a `Consumable`.

```ts
import { Consumer } from "watchmanrx";

const consumer = new Consumer<number>((self, value) => {
  console.log(value);
  self.next(); // grant credit for the next value
});

consumer.next(); // grant initial credit
consumer.push(1); // processed immediately
consumer.push(2); // queued, processed after handler calls next()

// drains the remaining queue at the consumer's own pace
consumer.terminate("complete");

//or immediately stops and clears the queue.
consumer.terminate("abort");
```

Beyond receiving and processing values, a `Consumer` is fully introspectable. Every internal moment is consumable as a first-class `Source` you can pipe and compose:

| Event              | Yields                                             |
| ------------------ | -------------------------------------------------- |
| `$push`            | Every value delivered to the consumer              |
| `$beforeConsuming` | Every value just before the handler runs           |
| `$afterConsuming`  | Every value just after the handler calls `.next()` |
| `$next`            | When the queue is exhausted and credit is granted  |
| `$drain`           | When the consumer enters drain state               |
| `$terminate`       | The termination reason                             |

A property `inProcessValue` holds the value currently being handled, accessible at any point during handler execution.

**Resource management** via `using` / `await using`:

```ts
{
  using consumer = new Consumer<number>(handler, options); // terminates with "abort" on scope exit
}
{
  await using consumer = new Consumer<number>(handler, options); // terminates with "complete" on scope exit
}
```

This makes a `Consumer` a self-contained execution unit you can monitor, trace, and build flow control around — without touching the pipeline itself.

### Source

> [API Reference →](./API_REF.md#sourcevalue--abstract-class)

Abstract base class for all sources, streams and transformers. Provides `.pipe()` for chaining, `.collect()` for terminal collection,`name` for tracing, and both `for await...of` and `for...of` iteration.

A `Source` must have an implementation so we provide a premade ones and they are suffixed by `Source`.

```ts
import { iterableSource, map, filter } from "watchmanrx";

const source = iterableSource([1, 2, 3, 4, 5]);

// pipe transformers
const result = source.pipe(filter((v) => v > 2)).pipe(map((v) => v * 10));

// async iteration
for await (const value of result) {
  console.log(value); // 30, 40, 50
}

//if the source is synchronous then you can consume it synchronously
for (const value of result) {
  console.log(value); // 30, 40, 50
}

// collect n values
const values = await result.collect(3); // [30, 40, 50]

// sync collect
const sync = result.collect(3, { sync: true }); // [30, 40, 50]
```

`Source.from(consumable)` wraps any `Consumable` in a `Source` proxy, giving it `.pipe()`,`collect()` and iteration without changing its behaviour. It is also used to expose a read-only view of a `Stream`.

### Stream

> [API Reference →](./API_REF.md#streamvalue--class-extends-sourcevalue)

A multicast push and/or pull source that broadcasts values to all its consumers. The primary building block for hot events and/or custom cold sources.

`Stream` is your first goto when you want to dispatch events or create a custom sources.
so its the only primitive you will work with most of the time, and you can wrap it with `Source.from()` to create a readonly source.

You can use it as an EventEmitter and you get **full** control over your events in a declarative way.

`Stream` can push on its own rate or consumed with the rate of the slowest or fastest consumers,and can even target a specific consumers to push in.

So in a nutshell `Stream` primitive is where this library converge and built for.

```ts
import { Stream, listen } from "watchmanrx";

//Hot stream (push based)
const stream = new Stream<number>();

stream.pipe(listen((v) => console.log("A:", v)));
stream.pipe(listen((v) => console.log("B:", v)));

stream.pushMany(1, 2, 3);
stream.terminate("complete");

// A cold and/or Hot stream (push and/or pull based).
const stream2 = new Stream({
  fastestConsumerNext(self, consumer) {
    //stream2 and self are the same instance
    self.push("hello: " + consumer.name);
    //you can even target some consumers
    if (consumer.name.startsWith("some name")) {
      consumer.push("some value");
    }
  },
  //and/or
  // slowestConsumerNext(stream, consumer){}
});

stream2.pipe(take(3)).pipe(toConsole());
stream2.pipe(take(3)).pipe(toConsole());
//hello: consumer_1
//hello: consumer_1
//hello: consumer_1
//hello: consumer_2
//hello: consumer_2
//hello: consumer_2
```

**Lifecycle events** are exposed as first-class `Source`s:

| Event                  | Yields                                        |
| ---------------------- | --------------------------------------------- |
| `$push`                | Every pushed value                            |
| `$fastestConsumerNext` | The fastest consumer that called `next()`     |
| `$slowestConsumerNext` | The slowest consumer that called `next()`     |
| `$consumerJoin`        | Every new consumer                            |
| `$consumerLeft`        | Every consumer that left                      |
| `$firstConsumerJoin`   | When the first consumer joins an empty stream |
| `$lastConsumerLeft`    | When the last consumer leaves                 |
| `$drain`               | When the stream enters drain state            |
| `$terminate`           | The termination reason                        |

> **Note** `Stream.Options` names match the lifecycle events names without the `$` prefix, and they are functions.

**Resource management** via `using` / `await using`:

```ts
{
  using stream = new Stream<number>(); // terminates with "abort" on scope exit
}
{
  await using stream = new Stream<number>(); // terminates with "complete" on scope exit
}
```

`Stream.from(consumable)` bridges any unicast `Consumable` into a multicast `Stream`. Upstream pulls are driven by the fastest or slowest consumer; if fastest is the driver then slower consumers buffer independently.

```ts
const shared = Stream.from(iterableSource([1, 2, 3]), {
  sourceDriver: "slowest", // or "fastest"
});
shared.pipe(listen((v) => console.log("A:", v)));
shared.pipe(listen((v) => console.log("B:", v)));
```

> **Note:** `sourceDriver` options has no effect when the source is pure hot (push only).

## Sources

> [API Reference →](./API_REF.md#sources)

Like we said before `Source` is an abstract class so we need to implement it.
Some of the sources are replayable — you can consume them multiple times and each consumer gets its own independent execution.

> **Note:** to have sharable source between all consumers use `share()` transformer or wrap the source with `Stream.from(source)`.

---

### `valuesSource(...values)`

> [API Reference →](./API_REF.md#valuessourcevalues--replayable)

Yields a fixed list of inline values then completes. Shorthand for `iterableSource(values)`.

```ts
valuesSource(1, 2, 3).pipe(listen(console.log)); // 1, 2, 3
```

### `iterableSource(iterable)`

> [API Reference →](./API_REF.md#iterablesourceiterable--replayable)

Wraps any `Iterable` as a pull-based source. A new iterator is created per consumer , so each consumer gets a fresh traversal. Also accepts a factory function returning an `Iterable`.

```ts
iterableSource([1, 2, 3]).pipe(listen(console.log));
iterableSource(() => new Set([1, 2, 3])).pipe(listen(console.log)); // factory form
```

### `asyncIterableSource(iterable)`

> [API Reference →](./API_REF.md#asynciterablesourceasynciterable--replayable)

Wraps any `AsyncIterable` as a pull-based source. A new async iterator is created per consumer. Also accepts a factory function returning an `AsyncIterable`.

```ts
async function* gen() {
  yield 1;
  yield 2;
}
asyncIterableSource(gen()).pipe(listen(console.log));
```

### `generatorSource(fn)`

> [API Reference →](./API_REF.md#generatorsourcefn--replayable)

Wraps a generator function as a pull-based source. The function is called once per consumer, so each consumer gets its own independent generator instance.

```ts
generatorSource(function* () {
  yield 1;
  yield 2;
  yield 3;
}).pipe(listen(console.log)); // 1, 2, 3
```

### `asyncGeneratorSource(fn)`

> [API Reference →](./API_REF.md#asyncgeneratorsourcefn--replayable)

Wraps an async generator function as a pull-based source. The function is called once per consumer, so each consumer gets its own independent async generator instance.

```ts
asyncGeneratorSource(async function* () {
  yield await fetch("/a");
  yield await fetch("/b");
}).pipe(listen(console.log));
```

### `iteratorSource(iterator)`

> [API Reference →](./API_REF.md#iteratorsourceiterator--replayable-factory-form)

Wraps a synchronous `Iterator` directly. Each `next()` call pulls one value. Terminates with `"complete"` when the iterator is done. Also accepts a factory function returning an `Iterator`. Note: if a raw iterator (not a factory) is passed, it is shared across all consumers.

```ts
iteratorSource([1, 2, 3].values()).pipe(listen(console.log)); // 1, 2, 3
iteratorSource(() => [1, 2, 3].values()).pipe(listen(console.log)); // factory form, replayable
```

### `asyncIteratorSource(iterator)`

> [API Reference →](./API_REF.md#asynciteratorsourceiterator)

Wraps an `AsyncIterator` directly. Each `next()` call pulls one value asynchronously. Also accepts a factory function returning an `AsyncIterator`. If a raw iterator is passed, it is shared across all consumers.

```ts
asyncIteratorSource(() => someAsyncIterator()).pipe(listen(console.log));
```

### `functionSource(fn)`

> [API Reference →](./API_REF.md#functionsourcefn--replayable)

Calls `fn` once on the first `next()`, yield the result, then completes. Useful for wrapping a single lazy computation as a source.

```ts
functionSource(() => Math.random()).pipe(listen(console.log)); // one random number
```

### `rangeSource(start, end)`

> [API Reference →](./API_REF.md#rangesourcestart-end--replayable)

Yields integers from `start` (inclusive) to `end` (exclusive), then completes.

```ts
rangeSource(1, 5).pipe(listen(console.log)); // 1, 2, 3, 4
```

### `intervalSource(ms)`

> [API Reference →](./API_REF.md#intervalsourcems--replayable)

Yields an incrementing counter (starting at `0`) every `ms` milliseconds via `setInterval`. Never completes on its own — must be terminated externally (e.g. with `take`, `takeUntil`, or `scope`).

```ts
intervalSource(500).pipe(take(3)).pipe(listen(console.log)); // 0, 1, 2
```

### `timeoutSource(ms, value?)`

> [API Reference →](./API_REF.md#timeoutsourcems-value--replayable)

Yields a single value after `ms` milliseconds via `setTimeout`, then completes. The yielded value defaults to `undefined` if not provided.

```ts
timeoutSource(1000).pipe(listen(() => console.log("done")));
timeoutSource(1000, "tick").pipe(listen(console.log)); // 'tick'
```

### `eventTargetSource(target, eventType)`

> [API Reference →](./API_REF.md#eventtargetsourcetarget-eventtype)

Yields DOM events of the given type from an `EventTarget`. A new event listener is registered per consumer and removed on termination. Fully typed for known DOM event types.

```ts
eventTargetSource(window, "click").pipe(listen((e) => console.log(e.type)));
eventTargetSource(window, "resize")
  .pipe(debounce(200))
  .pipe(listen(console.log));
```

### `abortSignalSource(signal)`

> [API Reference →](./API_REF.md#abortsignalsourcesignal)

Yields once when the given `AbortSignal` is aborted, then completes. If the signal is already aborted at registration time, yield immediately.

```ts
const controller = new AbortController();
abortSignalSource(controller.signal).pipe(listen(() => console.log("aborted")));
controller.abort();
```

### `abortControllerSource(controller)`

> [API Reference →](./API_REF.md#abortcontrollersourcecontroller)

Convenience wrapper around `abortSignalSource` that accepts an `AbortController` directly instead of its signal.

```ts
const controller = new AbortController();
abortControllerSource(controller).pipe(listen(() => console.log("aborted")));
controller.abort();
```

### `gcTokenSource(token)`

> [API Reference →](./API_REF.md#gctokensourcetoken)

Yields once when the given object is garbage collected, using `FinalizationRegistry`. If the object is already collected at registration time, yield immediately. Useful for tying stream lifetimes to object lifetimes.

```ts
let obj: object | null = {};
gcTokenSource(obj).pipe(listen(() => console.log("collected")));
obj = null; // allow GC
```

### `mergeSource(...sources)`

> [API Reference →](./API_REF.md#mergesourcesources)

Merges multiple sources into one, yielding values from all inputs as they arrive. Unlike the `merge` transformer, this is a standalone source that takes all inputs upfront. Terminates when the **last** input terminates.

```ts
const s1 = intervalSource(500).pipe(map(() => "A"));
const s2 = intervalSource(700).pipe(map(() => "B"));
mergeSource(s1, s2).pipe(listen(console.log)); // A, B, A, A, B, ...
```

### `combineSource(...sources)`

> [API Reference →](./API_REF.md#combinesourcesources)

Yields a tuple of the latest value from each input whenever any input yield. Uses `EMPTY` for inputs that have not yet yielded. Terminates when any input terminates. For `combineLatest` semantics, pipe `latest(1)` on each input before passing to `combineSource`.

```ts
const s1 = valuesSource(1, 2, 3).pipe(delay(100));
const s2 = valuesSource("a", "b").pipe(delay(200));
combineSource(s1, s2).pipe(listen(console.log));
// [1, EMPTY], [2, EMPTY], [2, 'a'], [3, 'a'], [3, 'b']
```

### `zipSource(...sources)`

> [API Reference →](./API_REF.md#zipsourcesources)

Combines values from multiple sources pairwise into tuples. Yields a tuple only when all inputs have provided a new value. Terminates when any input terminates. Unmatched values remaining at termination are available on `.$rest`.

```ts
const s1 = valuesSource(1, 2, 3);
const s2 = valuesSource("a", "b", "c");
zipSource(s1, s2).pipe(listen(console.log)); // [1,'a'], [2,'b'], [3,'c']
```

## Streams

> [API Reference →](./API_REF.md#streams)

| Stream      | Description                                                                   |
| ----------- | ----------------------------------------------------------------------------- |
| `Stream<T>` | Multicast push source. The base hot stream                                    |
| `Signal<T>` | A `Stream` that terminates after the first push. For one-shot events          |
| `State<T>`  | A `Stream` with a `.value` property. Setting `.value` pushes to all consumers |

## Transformers

> [API Reference →](./API_REF.md#transformers)

Transformers are just a `Source` that accept a `Consumable` on their constructors. they are classes so you can import and instantiate them for a step by step transformations , they expose a factories that are `.pipe` compatible.

Most available transformers reuse and return the upstream consumer, they are transducers(all transformations collapse into one execution) so you are free to create as many pipelines of any size.

---

### Filtering

> [API Reference →](./API_REF.md#filtering)

#### `filter(predicate, complement?)` — [ref](./API_REF.md#filterpredicate-complement)

Passes only values that satisfy the predicate. Rejected values are forwarded to the lazy `.$complements` source at zero cost if unused. The `.$complements` source is passive relative to `filter` — consuming it never drives the upstream.

```ts
const f = valuesSource(1, 2, 3, 4).pipe(filter((v) => v % 2 === 0));
f.$complements.pipe(listen(console.log)); // 1, 3
f.pipe(listen(console.log)); // 2, 4

//or stepped approach
const source = valuesSource(1, 2, 3, 4);
const f = new Filter(source, (v) => v % 2 === 0);
f.$complements.pipe(listen(console.log)); // 1, 3
f.pipe(listen(console.log)); // 2, 4
```

An optional inline `complement` callback can be passed as a second argument instead of consuming `.$complements`.

#### `take(n)` — [ref](./API_REF.md#taken)

Takes the first `n` values then terminates.

```ts
intervalSource(100).pipe(take(3)).pipe(listen(console.log)); // 0, 1, 2
```

#### `takeWhile(predicate)` — [ref](./API_REF.md#takewhilepredicate)

Takes values while the predicate returns `true`. Terminates on the first `false`.

```ts
valuesSource(1, 2, 3, 4)
  .pipe(takeWhile((v) => v < 3))
  .pipe(listen(console.log)); // 1, 2
```

#### `takeUntil($notifier)` — [ref](./API_REF.md#takeuntilnotifier)

Takes values until the notifier yield, then terminates. Useful for tying a stream's lifetime to an external event.

```ts
const stop = timeoutSource(1000);
intervalSource(200).pipe(takeUntil(stop)).pipe(listen(console.log));
```

#### `skip(n)` — [ref](./API_REF.md#skipn)

Skips the first `n` values, then passes all subsequent values through.

```ts
valuesSource(1, 2, 3, 4).pipe(skip(2)).pipe(listen(console.log)); // 3, 4
```

#### `skipWhile(predicate)` — [ref](./API_REF.md#skipwhilepredicate)

Skips values while the predicate returns `true`. Once the predicate returns `false`, all subsequent values pass through without re-checking.

```ts
valuesSource(1, 2, 3, 4)
  .pipe(skipWhile((v) => v < 3))
  .pipe(listen(console.log)); // 3, 4
```

#### `skipUntil($notifier)` — [ref](./API_REF.md#skipuntilnotifier)

Skips all values until the notifier yield, then passes all subsequent values through.

```ts
const start = timeoutSource(600);
stream.pipe(skipUntil(start)).pipe(listen(console.log));
```

#### `distinct(keySelector?, $flushes?)` — [ref](./API_REF.md#distinctkeyselector-flushes)

Filters out duplicate values using a `Set`. Optionally accepts a `keySelector` to compare by a derived key. A `$flushes` notifier resets the seen-set when it yield.

```ts
valuesSource(1, 1, 2, 2, 3).pipe(distinct()).pipe(listen(console.log)); // 1, 2, 3

// by key
valuesSource({ id: 1 }, { id: 1 }, { id: 2 })
  .pipe(distinct((v) => v.id))
  .pipe(listen(console.log)); // {id:1}, {id:2}

// with flush
const reset = new Signal();
stream.pipe(distinct(undefined, reset)).pipe(listen(console.log));
reset.push(); // clears the seen-set
```

#### `first(predicate?)` — [ref](./API_REF.md#firstpredicate)

Yields the first value then terminates. Optionally accepts a predicate — yield the first value for which it returns `true`. Yields `EMPTY` if the source completes without a match.

```ts
valuesSource(1, 2, 3).pipe(first()).pipe(listen(console.log)); // 1
valuesSource(1, 2, 3)
  .pipe(first((v) => v > 1))
  .pipe(listen(console.log)); // 2
```

#### `last()` — [ref](./API_REF.md#last)

Yields the last value when the source completes. Yields `EMPTY` if the source completes without yielding. Combine with aggregation transformers like `count`, `sum`, or `scan` to get their final result.

```ts
valuesSource(1, 2, 3).pipe(last()).pipe(listen(console.log)); // 3
```

#### `range(start, offset)` — [ref](./API_REF.md#rangestart-offset)

Passes only values at indices `[start, start + offset]` (0-based). Values before `start` are skipped; the stream terminates after `offset` values.

```ts
valuesSource("a", "b", "c", "d", "e")
  .pipe(range(1, 3))
  .pipe(listen(console.log)); // b, c, d
```

---

### Transformation

> [API Reference →](./API_REF.md#transformation)

#### `map(fn)` — [ref](./API_REF.md#mapfn)

Transforms each value using `(value, index) => result`. The index is per-consumer .

```ts
valuesSource(1, 2, 3)
  .pipe(map((v, i) => `${i}:${v}`))
  .pipe(listen(console.log)); // 0:1, 1:2, 2:3
```

#### `flat(depth?)` — [ref](./API_REF.md#flatdepth)

Flattens a stream of arrays into individual values. `depth` controls how deep to flatten (default `0` = one level, equivalent to `Array.flat(1)`).

```ts
valuesSource([1, 2], [3, 4]).pipe(flat()).pipe(listen(console.log)); // 1, 2, 3, 4
valuesSource([[1, 2], [3]])
  .pipe(flat(1))
  .pipe(listen(console.log)); // 1, 2, 3
```

#### `flat$(depth?)` — [ref](./API_REF.md#flatdepth-1)

Flattens a stream of `Consumable`s sequentially — consumes each inner `Consumable` and waits for it to complete before moving to the next. `depth` controls how many levels of nesting to flatten (default `1`).

```ts
valuesSource(valuesSource(1, 2), valuesSource(3, 4))
  .pipe(flat$())
  .pipe(listen(console.log)); // 1, 2, 3, 4
```

#### `scan(seed, reducer)` — [ref](./API_REF.md#scanseed-reducer)

Accumulates values with a reducer and yield the running accumulator after each value — like `Array.reduce` but streaming. Pipe `last()` after it to get only the final result.

```ts
valuesSource(1, 2, 3)
  .pipe(scan(0, (acc, v) => acc + v))
  .pipe(listen(console.log)); // 1, 3, 6

// final value only
valuesSource(1, 2, 3)
  .pipe(scan(0, (acc, v) => acc + v))
  .pipe(last())
  .pipe(listen(console.log)); // 6
```

#### `scanArray()` — [ref](./API_REF.md#scanarray)

Accumulates all values into a growing array and yield the same array reference after each push. The array is mutated in place for performance. Use `last()` to get the final collected array.

```ts
valuesSource(1, 2, 3).pipe(scanArray()).pipe(last()).pipe(listen(console.log)); // [1, 2, 3]
```

#### `resolve(concurrency?)` — [ref](./API_REF.md#resolveconcurrency)

Resolves a stream of `Promise`s. Yields resolved values or `Error<reason>` for rejections. The `concurrency` parameter (default `1`) controls how many promises can be in-flight simultaneously. Upstream credit is withheld until a slot is free, meaning the upstream producer is not even called until a slot opens — so there will be at most exactly `concurrency` promises in flight at any time, never more.

```ts
valuesSource("url_1", "url_2", "url_3")
  .pipe(map((url) => fetch(url).then((response) => response.json())))
  .pipe(resolve(2))
  .pipe(listen(console.log));
// at most 2 fetches are awaited concurrently.
// until one of them resolve and consumed then the third will be triggered.
```

#### `safe(fn)` — [ref](./API_REF.md#safefn)

Wraps a transformer pipeline in a try/catch. On success, values pass through unchanged. On a thrown error, yield `Error<thrown>` instead of crashing the pipeline.

```ts
valuesSource(1, 2, 3)
  .pipe(
    safe(
      map((v) => {
        if (v === 2) throw "bad";
        return v;
      }),
    ),
  )
  .pipe(listen(console.log)); // 1, Error('bad'), 3
```

#### `context(ctx)` — [ref](./API_REF.md#contextctx)

Wraps each value with a shared mutable context object, yielding `{ value, context }`. The same context reference is shared across all values in the stream.

```ts
valuesSource(1, 2, 3)
  .pipe(context({ total: 0 }))
  .pipe(tap(({ value, context }) => (context.total += value)))
  .pipe(listen(({ value, context }) => console.log(value, context.total)));
// 1 1, 2 3, 3 6
```

---

### Aggregation

> [API Reference →](./API_REF.md#aggregation)

All aggregation transformers yield a running value after each input — not just at the end so you can track the progress. Pipe `last()` after any of them to get only the final result.

#### `count()` — [ref](./API_REF.md#count)

Yields the running count after each value.

```ts
valuesSource("a", "b", "c").pipe(count()).pipe(listen(console.log)); // 1, 2, 3
valuesSource("a", "b", "c")
  .pipe(count())
  .pipe(last())
  .pipe(listen(console.log)); // 3
```

#### `sum()` — [ref](./API_REF.md#sum)

Yields the running sum after each numeric value.

```ts
valuesSource(1, 2, 3).pipe(sum()).pipe(last()).pipe(listen(console.log)); // 6
```

#### `min()` — [ref](./API_REF.md#min)

Yields the running minimum after each numeric value.

```ts
valuesSource(3, 1, 4, 1, 5).pipe(min()).pipe(last()).pipe(listen(console.log)); // 1
```

#### `max()` — [ref](./API_REF.md#max)

Yields the running maximum after each numeric value.

```ts
valuesSource(3, 1, 4, 1, 5).pipe(max()).pipe(last()).pipe(listen(console.log)); // 5
```

#### `every(predicate)` — [ref](./API_REF.md#everypredicate)

Yields `true` while all values satisfy the predicate. Yields `false` and terminates immediately on the first failure.

```ts
valuesSource(1, 2, 3)
  .pipe(every((v) => v > 0))
  .pipe(last())
  .pipe(listen(console.log)); // true
valuesSource(1, -1, 3)
  .pipe(every((v) => v > 0))
  .pipe(last())
  .pipe(listen(console.log)); // false
```

#### `batch(size)` — [ref](./API_REF.md#batchsize)

Collects values into arrays of up to `size`. Yields a partial batch with any remaining values on stream completion.

```ts
valuesSource(1, 2, 3, 4, 5).pipe(batch(2)).pipe(listen(console.log)); // [1,2], [3,4], [5]
```

#### `buffer(size, startBufferEvery?)` — [ref](./API_REF.md#buffersize-startbufferevery)

Collects values into fixed-size sliding-window arrays. A new buffer starts every `startBufferEvery` values (defaults to `size`, giving non-overlapping windows). Incomplete buffers are yielded on stream completion.

```ts
// non-overlapping
valuesSource(1, 2, 3, 4, 5).pipe(buffer(3)).pipe(listen(console.log)); // [1,2,3], [4,5]

// sliding window
valuesSource(1, 2, 3, 4).pipe(buffer(3, 1)).pipe(listen(console.log)); // [1,2,3], [2,3,4], [3,4], [4]
```

#### `zip(...others)` — [ref](./API_REF.md#zipothers)

Combines values from multiple sources pairwise into tuples. Yields a tuple only when all inputs have provided a new value. Terminates when any input terminates. Unmatched values remaining at termination are available on `.$rest`.

```ts
const s1 = new Stream<number>();
const s2 = new Stream<string>();
s1.pipe(zip(s2)).pipe(listen(console.log));
s1.push(1);
s2.push("a"); // [1, 'a']
s1.push(2);
s2.push("b"); // [2, 'b']
```

#### `combine(...others)` — [ref](./API_REF.md#combineothers)

Yields a tuple of the latest value from each input whenever any input yield. Uses `EMPTY` for inputs that have not yet yielded. Terminates when any input terminates. For `combineLatest` semantics, pipe `latest(1)` on each input before combining.

```ts
const s1 = valuesSource(1, 2, 3).pipe(delay(100));
const s2 = valuesSource("a", "b").pipe(delay(200));
s1.pipe(combine(s2)).pipe(listen(console.log));
// [1, EMPTY], [2, EMPTY], [2, 'a'], [3, 'a'], [3, 'b']
```

#### `merge(...others)` — [ref](./API_REF.md#mergeothers)

Merges multiple sources into one, yielding values from all inputs as they arrive. Terminates when the primary input terminates.

```ts
const s1 = intervalSource(500).pipe(map(() => "A"));
const s2 = intervalSource(700).pipe(map(() => "B"));
s1.pipe(merge(s2)).pipe(listen(console.log)); // A, B, A, A, B, ...
```

---

### Flow Control

> [API Reference →](./API_REF.md#flow-control)

#### `debounce(ms)` — [ref](./API_REF.md#debouncems)

Yields the latest value only after `ms` milliseconds of silence. Each new incoming value resets the timer. Useful for search inputs or resize events.

```ts
stream.pipe(debounce(300)).pipe(listen(console.log));
```

#### `delay(ms)` — [ref](./API_REF.md#delayms)

Delays each value by `ms` milliseconds using `setTimeout`. If `ms <= 0`, uses `queueMicrotask` instead.

```ts
valuesSource(1, 2, 3).pipe(delay(500)).pipe(listen(console.log));
```

#### `delayOnce(ms)` — [ref](./API_REF.md#delayoncems)

Delays only the first value by `ms` milliseconds. All subsequent values pass through immediately.

```ts
valuesSource(1, 2, 3).pipe(delayOnce(500)).pipe(listen(console.log)); // 1 delayed, 2 and 3 immediate
```

#### `pace(ms)` — [ref](./API_REF.md#pacems)

Rate-limits output to at most one value per `ms` milliseconds. The timer is decoupled from emission: if a value arrives after the window has already expired, it is yielded directly with no delay. If a value arrives before the window expires, it is scheduled for the next available slot. Credit flows back to slow the upstream producer.

```ts
// fast upstream — values are spaced out to 500ms apart
intervalSource(50).pipe(pace(500)).pipe(listen(console.log));

// slow upstream — values that arrive after the window pass through immediately
intervalSource(1000).pipe(pace(500)).pipe(listen(console.log));
```

#### `gate($control)` — [ref](./API_REF.md#gatecontrol)

Opens or closes the stream based on a boolean control source. `true` opens the gate and starts consuming the input; `false` closes it and terminates the inner consumer. The gate re-opens on the next `true`.

```ts
const toggle = intervalSource(3000).pipe(map((_, i) => i % 2 === 0));
intervalSource(500).pipe(gate(toggle)).pipe(listen(console.log));
// values flow for 3s, stop for 3s, flow for 3s, ...
```

#### `passive()` — [ref](./API_REF.md#passive)

Registers to the upstream without calling `next()`, so it never drives upstream pulls. Values are only received when the upstream pushes them independently or triggered by another consumer.

To understand why this matters: on a shared stream, the fastest/slower consumer (defined on `sourceDriver` options) is the one that drives the source — when it calls `next()`, the shared source pulls the next value and broadcasts it to all consumers. Faster consumers process it immediately; slower ones enqueue it and drain at their own pace. If an observer consumer also calls `next()`, it competes to become the driver and pulls values faster than the intended driver would. When the intended driver is slow and cannot afford to miss values (so throttling transformers like `debounce` or `latest` are not an option), this extra pressure causes values to pile up in the slow consumer's queue, which can grow without bound and eventually exhaust memory. A passive consumer opts out entirely: it registers to receive values but never calls `next()`, so it never becomes a potential driver. It only receives values that the driver consumer already triggered, at exactly that consumer's pace.

This is useful when you want to observe a shared stream without affecting its throughput.

```ts
const shared = valuesSource(1, 2, 3, 4).pipe(share());

// this consumer only observes — never calls next(), never pulls
shared.pipe(passive()).pipe(listen((v) => console.log("observer:", v)));

// this consumer drives the source — calls next() after each value
shared.pipe(listen((v) => console.log("driver:", v)));
```

#### `pump()` — [ref](./API_REF.md#pump)

Eagerly drains the upstream by pre-granting credit, pushing all values into an internal `Stream`. Useful for activating a dormant pipeline that has no terminal consumer yet.

```ts
valuesSource(1, 2, 3)
  .pipe(map((v) => v * 2))
  .pipe(tap(console.log))
  .pipe(pump());
```

#### `share()` — [ref](./API_REF.md#share)

Converts a pull-based replayable source into a single shared execution via `Stream.from`. Without `share()`, each consumer of a replayable source gets its own independent run — its own iterator, its own timer, its own generator instance. `share()` collapses that into one execution that broadcasts to all consumers.

Push-based sources like `eventTargetSource` or `abortSignalSource` don't need `share()` — each consumer already consumes independently to the same live event, so there's no shared execution to coordinate. `share()` is specifically meaningful with pull-based sources like `iterableSource`, `generatorSource`, `rangeSource`, `intervalSource`, and similar.

Note: with synchronous replayable sources, the first eager consumer will exhaust the source before others can consume — register all consumers before triggering consumption.

```ts
const shared = intervalSource(500).pipe(share());
shared.pipe(listen((v) => console.log("A", v)));
shared.pipe(listen((v) => console.log("B", v)));
```

#### `latest(n)` — [ref](./API_REF.md#latestn)

Buffers the last `n` values. Late consumers receive the buffered values immediately on registration, then continue with live values. Requires `share()` before it when used with replayable sources, otherwise the source replays from the beginning for each consumer.

```ts
const buffered = intervalSource(300).pipe(share()).pipe(latest(2));
// consume 2 seconds later — immediately receives the 2 most recent values
setTimeout(() => buffered.pipe(listen(console.log)), 2000);
```

#### `switch$()` — [ref](./API_REF.md#switch)

Registers to the latest inner `Consumable` yielded by the upstream, cancelling the previous one when a new one arrives. Non-`Consumable` values are passed through directly. Best used with hot sources — replayable sources may be exhausted before the inner consumer attaches.

```ts
const stream = new Stream<Consumable<number> | number>();
stream.pipe(switch$()).pipe(listen(console.log));

stream.push(intervalSource()); // starts consuming inner

await new Promise((r) => setTimeout(r));
stream.push(intervalSource()); // cancels previous, starts new

await new Promise((r) => setTimeout(r));
stream.push(99); // plain value,cancels previous and passed through
```

---

### Side Effects & Termination

> [API Reference →](./API_REF.md#side-effects--termination)

#### `tap(fn)` — [ref](./API_REF.md#tapfn)

Runs a side-effect callback `(value, index) => void` for each value without modifying it. The value passes through unchanged.

```ts
valuesSource(1, 2, 3)
  .pipe(tap((v, i) => console.log("side effect", i, v)))
  .pipe(listen());
```

#### `tapInput(fn)` — [ref](./API_REF.md#tapinputfn)

Runs a side-effect with access to the raw input source before it enters the pipeline. Returns the input unchanged. Useful for tapping into side-channel streams like `filter.$complements` without breaking the chain.

```ts
source
  .pipe(filter((v) => v > 2))
  .pipe(tapInput((f) => f.$complements.pipe(listen(console.log))))
  .pipe(listen(console.log));
```

#### `tapTerminate(fn)` — [ref](./API_REF.md#tapterminatefn)

Runs a callback when the stream terminates, receiving the termination reason (`"complete"` or `"abort"`). Values pass through unchanged.

```ts
valuesSource(1, 2, 3)
  .pipe(tapTerminate((reason) => console.log("done:", reason)))
  .pipe(listen());
// done: complete
```

#### `listen(fn?)` — [ref](./API_REF.md#listenfn)

Hot driver — consumes the source, calls `fn` for each value, and automatically grants credit after each call. Returns the input source for further chaining. The callback is optional if you only want to drive consumption.

```ts
valuesSource(1, 2, 3).pipe(listen(console.log)); // 1, 2, 3
valuesSource(1, 2, 3).pipe(listen()); // drives consumption, no callback
```

#### `terminate()` — [ref](./API_REF.md#terminate)

Ignores all values and yield the termination reason (`"complete"` or `"abort"`) as a single value when the stream ends.

```ts
valuesSource(1, 2, 3).pipe(terminate()).pipe(listen(console.log)); // 'complete'
```

#### `scope(...others)` — [ref](./API_REF.md#scopeothers)

Terminates the pipeline when any of the provided notifier sources terminates (either `complete` or `abort`). The first notifier to terminate wins.

```ts
const lifetime = timeoutSource(5000);
intervalSource(500).pipe(scope(lifetime)).pipe(listen(console.log)); // stops after 5s
```

#### `scopeStrict(...others)` — [ref](./API_REF.md#scopestrictothers)

Like `scope`, but requires all notifiers to terminate before the pipeline terminates.

```ts
const s1 = timeoutSource(1000);
const s2 = timeoutSource(2000);
intervalSource(300).pipe(scopeStrict(s1, s2)).pipe(listen(console.log)); // stops after 2s
```

#### `print(label?)` — [ref](./API_REF.md#printlabel)

Logs each value to the console with an optional label prefix, then passes it through unchanged. Values continue downstream.

```ts
valuesSource(1, 2, 3).pipe(print("val:")).pipe(listen()); // val: 1, val: 2, val: 3
```

#### `toConsole(label?)` — [ref](./API_REF.md#toconsolelabel)

Prints each value to the console and eagerly drains the stream via `pump()`.

```ts
valuesSource(1, 2, 3).pipe(toConsole("result:")); // result: 1, result: 2, result: 3
```

## Protocol

The communication model is formally specified here [EGSP ](https://github.com/soffinal/egsp).

## License

MIT
