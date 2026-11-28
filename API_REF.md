# WatchmanRx API Reference

## Core

### `Consumable<VALUE>` — interface

The base contract. Any object with a `consume` method is a `Consumable`.

```ts
interface Consumable<VALUE> {
  readonly name: string;
  consume(
    handler: Consumer.Handler<VALUE>,
    options?: Consumer.Options<VALUE>,
  ): Consumer<VALUE>;
}
```

---

### `Consumer<VALUE>` — class

The fundamental unit of consumption. Controls throughput via a credit system.
Implements `Disposable` and `AsyncDisposable`

**Constructor**

```ts
new Consumer<VALUE>(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>)
```

**`Consumer.Options<VALUE>`**

| Option            | Type                              | Description                         |
| ----------------- | --------------------------------- | ----------------------------------- |
| `name`            | `string`                          | Name override                       |
| `queueFactory`    | `() => Queue<VALUE>`              | Custom queue                        |
| `init`            | `(c) => InitCleanup \| undefined` | Setup; return cleanup fn            |
| `push`            | `(c, value) => void`              | Called on every push                |
| `beforeConsuming` | `(c, value) => void`              | Before handler runs                 |
| `afterConsuming`  | `(c, value) => void`              | After handler calls next            |
| `next`            | `(c) => void`                     | When credit granted and queue empty |
| `drain`           | `(c) => void`                     | When entering drain state           |
| `terminate`       | `(c, reason) => void`             | On termination                      |

**Properties**

| Property         | Type                        | Description                                    |
| ---------------- | --------------------------- | ---------------------------------------------- |
| `name`           | `string`                    | Consumer name for tracing                      |
| `handler`        | `Consumer.Handler<VALUE>`   | Current handler (replaceable)                  |
| `options`        | `Consumer.Options<VALUE>`   | Current options                                |
| `status`         | `Consumer.Status`           | `"active" \| "drain" \| "complete" \| "abort"` |
| `credit`         | `number`                    | Available credits                              |
| `queue`          | `Queue<VALUE> \| undefined` | Internal buffer                                |
| `inProcessValue` | `VALUE \| Empty`            | Value currently in handler                     |

**Methods**

| Method                | Description                                                       |
| --------------------- | ----------------------------------------------------------------- |
| `push(value)`         | Deliver a value; processed immediately if credit > 0, else queued |
| `pushMany(...values)` | Push inline values                                                |
| `pushBatch(values[])` | Push an array of values                                           |
| `next()`              | Grant one credit                                                  |
| `terminate(reason)`   | `"abort"` clears queue immediately; `"complete"` drains first     |

**Event Sources**

| Source             | Emits                                          |
| ------------------ | ---------------------------------------------- |
| `$push`            | Every value delivered                          |
| `$beforeConsuming` | Every value just before handler runs           |
| `$afterConsuming`  | Every value just after handler calls `.next()` |
| `$next`            | When queue is exhausted and credit is granted  |
| `$drain`           | When consumer enters drain state               |
| `$terminate`       | The termination reason                         |

**Resource management**

```ts
{
  using consumer = new Consumer(handler);
} // terminates "abort" on scope exit
{
  await using consumer = new Consumer(handler);
} // terminates "complete" on scope exit
```

---

### `Source<VALUE>` — abstract class

Abstract base for all sources, streams, and transformers. Implements `Consumable<VALUE>`, `AsyncIterable<VALUE>`, and `Iterable<VALUE>`.

**Implements `Consumable<VALUE>`**

| Method    | Signature                                     | Description                                  |
| --------- | --------------------------------------------- | -------------------------------------------- |
| `consume` | `consume(handler, options?): Consumer<VALUE>` | Abstract — must be implemented by subclasses |

**Properties**

| Property | Type     | Description             |
| -------- | -------- | ----------------------- |
| `name`   | `string` | Source name for tracing |

**Methods**

| Method                   | Signature                                    | Description                       |
| ------------------------ | -------------------------------------------- | --------------------------------- |
| `pipe`                   | `pipe<O>(transform: ($input: this) => O): O` | Chain a transformer               |
| `collect`                | `collect(n?, options?)`                      | Collect `n` values sync or async  |
| `[Symbol.asyncIterator]` | —                                            | `for await...of` support          |
| `[Symbol.iterator]`      | —                                            | `for...of` support (sync sources) |

**Static**

| Method                    | Description                                                                                                                                                                      |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Source.from(consumable)` | Wraps any `Consumable` in a `Source` proxy — gives it `.pipe()`, `.collect()`, and iteration without changing its behaviour. Also used to expose a read-only view of a `Stream`. |

**`Source.CollectOptions<SYNC>`**

```ts
{ sync?: SYNC }
```

```ts
const value = await source.collect(); // one value (async)
const values = await source.collect(3); // [v1, v2, v3] (async)
const value = source.collect(1, { sync: true }); // sync
const values = source.collect(3, { sync: true }); // sync
```

---

### `Stream<VALUE>` — class extends `Source<VALUE>`

Multicast push/pull source. Broadcasts/yield to all consumers.

**Constructor**

```ts
new Stream<VALUE>(options?: Stream.Options<VALUE>)
```

**`Stream.Options<VALUE>`**

| Option                | Type                                                                                      | Description                                            |
| --------------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| `name`                | `string`                                                                                  | Name override                                          |
| `consumerSetFactory`  | `() => ConsumerSet<VALUE>`                                                                | Custom ConsumerSet                                     |
| `consumerFactory`     | `(handler: Consumer.Handler<VALUE>,options?: Consumer.Options<VALUE>) => Consumer<VALUE>` | Custom Consumer fabric                                 |
| `init`                | `(stream: Stream<VALUE>) => undefined \| ((reason: TerminateReason) => void)`             | Setup; return cleanup fn                               |
| `push`                | `(stream: Stream<VALUE>, value: VALUE) => void`                                           | Called on every push                                   |
| `fastestConsumerNext` | `(stream: Stream<VALUE>,consumer: Consumer<VALUE>) => void`                               | Faster consumer that call request data                 |
| `slowestConsumerNext` | `(stream: Stream<VALUE>,consumer: Consumer<VALUE>) => void`                               | Slowest or last consumer that request data             |
| `consumerJoin`        | `(stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void`                              | When consumer is created                               |
| `consumerLeft`        | `(stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void`                              | When consumer terminate                                |
| `firstConsumerJoin`   | `(stream: Stream<VALUE>,consumer: Consumer<VALUE>) => void`                               | When the first consumer is created                     |
| `lastConsumerJoin`    | `(stream: Stream<VALUE>,consumer: Consumer<VALUE>) => void`                               | When the last consumer is terminated                   |
| `drain`               | `(stream: Stream<VALUE>) => void`                                                         | When Stream enter drain mode via terminate("complete") |
| `terminate`           | `(stream: Stream<VALUE>, reason: TerminateReason) => void`                                | When Stream terminate                                  |

**Properties**

| Property      | Type                    | Description                                    |
| ------------- | ----------------------- | ---------------------------------------------- |
| `status`      | `Stream.Status`         | `"active" \| "drain" \| "complete" \| "abort"` |
| `consumerSet` | `ConsumerSet<VALUE>`    | Manages all consumers                          |
| `options`     | `Stream.Options<VALUE>` | Current options                                |

**Event Sources**

| Source                 | Emits                                     |
| ---------------------- | ----------------------------------------- |
| `$push`                | Every pushed value                        |
| `$fastestConsumerNext` | The fastest consumer that called `next()` |
| `$slowestConsumerNext` | The slowest consumer that called `next()` |
| `$consumerJoin`        | Every new consumer                        |
| `$consumerLeft`        | Every consumer that left                  |
| `$firstConsumerJoin`   | When first consumer joins an empty stream |
| `$lastConsumerLeft`    | When last consumer leaves                 |
| `$drain`               | When stream enters drain state            |
| `$terminate`           | The termination reason                    |

**Methods**

| Method                       | Description                |
| ---------------------------- | -------------------------- |
| `push(value)`                | Broadcast to all consumers |
| `pushMany(...values)`        | Push inline values         |
| `pushBatch(values[])`        | Push an array of values    |
| `consume(handler, options?)` | Create a new consumer      |
| `terminate(reason)`          | `"abort"` or `"complete"`  |

**Static**

```ts
Stream.from(consumable, options?: Stream.Options & { sourceDriver?: "slowest" | "fastest" }): Stream<VALUE>
```

Bridges any unicast `Consumable` into a multicast `Stream`.

**Stream.FromOptions<VALUE>**
Extends `Stream.options<VALUE>`

```ts
  {
    sourceDriver?: SourceDriver;
  }
```

**Resource management**

```ts
{
  using stream = new Stream();
} // terminates "abort"
{
  await using stream = new Stream();
} // terminates "complete"
```

---

### `Queue<VALUE>` — interface

FIFO queue used internally by `Consumer`.

| Member           | Description                             |
| ---------------- | --------------------------------------- |
| `enqueue(value)` | Add a value                             |
| `dequeue()`      | Remove and return next value or `EMPTY` |
| `clear()`        | Remove all values                       |
| `size`           | Number of queued values                 |

---

### `ConsumerSet<VALUE>` — interface

Manages the set of consumers a `Stream` broadcasts to.

| Member              | Description             |
| ------------------- | ----------------------- |
| `size`              | Number of consumers     |
| `push(value)`       | Broadcast to all        |
| `add(consumer)`     | Register a consumer     |
| `delete(consumer)`  | Remove a consumer       |
| `terminate(reason)` | Terminate all consumers |

---

### Constants & Types

| Export              | Description                                                                    |
| ------------------- | ------------------------------------------------------------------------------ |
| `EMPTY`             | `Symbol.for("empty")` — sentinel for absence of value                          |
| `TerminateReason`   | `"abort" \| "complete"`                                                        |
| `SourceDriver`      | `"slowest" \| "fastest"`                                                       |
| `Error<VALUE>`      | Typed error wrapper: `new Error(value)` — carries a value instead of a message |
| `isConsumable(obj)` | Type guard: returns `true` if `obj` is a `Consumable`                          |

**Utility types**

| Type                           | Description                                                        |
| ------------------------------ | ------------------------------------------------------------------ |
| `ValueOfConsumable<T, DEPTH?>` | Extracts value type from a `Consumable`, recursively up to `DEPTH` |
| `ValueOfPromise<T, DEPTH?>`    | Extracts resolved type from a `Promise`                            |
| `ValueOfArray<T, DEPTH?>`      | Extracts element type from an `Array`                              |
| `ZipedArray<T[]>`              | Tuple of value types from a tuple of `Consumable`s                 |
| `SizedArray<VALUE, SIZE>`      | Tuple of `VALUE` repeated `SIZE` times                             |
| `Prettify<T>`                  | Flattens an object type for IDE display                            |

---

## Streams

### `Signal<VALUE>` — class extends `Stream<VALUE>`

A `Stream` that terminates after the first `push`.

```ts
new Signal<VALUE>(options?: Stream.Options<VALUE>)
signal<VALUE>(options?: Stream.Options<VALUE>): Signal<VALUE>
```

```ts
const sig = new Signal<string>();
sig.pipe(listen(console.log));
sig.push("done"); // emits then terminates
```

---

### `State<VALUE>` — class extends `Stream<VALUE>`

A `Stream` with a `.value` property. Setting `.value` pushes to all consumers. Note: calling `.push()` directly does NOT update `.value` but can be useful for notifications.

```ts
new State<VALUE>(initialValue: VALUE, options?: Stream.Options<VALUE>)
state<VALUE>(initialValue: VALUE, options?: Stream.Options<VALUE>): State<VALUE>
```

| Property | Description                                |
| -------- | ------------------------------------------ |
| `value`  | Get current value or set to push a new one |

```ts
const count = state(0);
count.pipe(listen((v) => console.log(v)));
count.value = 1; // logs 1
```

---

## Sources

All sources extend `Source<VALUE>` and support `.pipe()`, `.collect()`, and iteration. Sources marked **Replayable** create a fresh execution per consumer.

---

### `valuesSource(...values)` — Replayable

Emits a fixed list of values then completes.

```ts
valuesSource<VALUES extends [any, ...any[]]>(...values: VALUES): IterableSource<VALUE>
```

---

### `iterableSource(iterable)` — Replayable

Wraps any `Iterable`. New iterator per consumer.

```ts
iterableSource<VALUE>(iterable: Iterable<VALUE> | (() => Iterable<VALUE>)): IterableSource<VALUE>
```

---

### `asyncIterableSource(asyncIterable)` — Replayable

Wraps any `AsyncIterable`. New async iterator per consumer.

```ts
asyncIterableSource<VALUE>(asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>)): AsyncIterableSource<VALUE>
```

---

### `generatorSource(fn)` — Replayable

Wraps a generator function. New generator per consumer.

```ts
generatorSource<VALUE>(generatorFn: () => Generator<VALUE>): GeneratorSource<VALUE>
```

---

### `asyncGeneratorSource(fn)` — Replayable

Wraps an async generator function. New generator per consumer.

```ts
asyncGeneratorSource<VALUE>(asyncGeneratorFn: () => AsyncGenerator<VALUE>): AsyncGeneratorSource<VALUE>
```

---

### `iteratorSource(iterator)` — Replayable (factory form)

Wraps a synchronous `Iterator`. If a raw iterator is passed it is shared; pass a factory for replayable behavior.

```ts
iteratorSource<VALUE>(iterator: Iterator<VALUE> | (() => Iterator<VALUE>)): IteratorSource<VALUE>
```

---

### `asyncIteratorSource(iterator)`

Wraps an `AsyncIterator`. Factory form is replayable; raw iterator is shared.

```ts
asyncIteratorSource<VALUE>(asyncIterator: AsyncIterator<VALUE> | (() => AsyncIterator<VALUE>)): AsyncIteratorSource<VALUE>
```

---

### `functionSource(fn)` — Replayable

Calls `fn` once, emits the result, then completes.

```ts
functionSource<VALUE>(fn: (...args: any[]) => VALUE): FunctionSource<VALUE>
```

---

### `rangeSource(start, end)` — Replayable

Emits integers from `start` (inclusive) to `end` (exclusive).

```ts
rangeSource(start: number, end: number): RangeSource
```

---

### `intervalSource(ms)` — Replayable

Emits an incrementing counter every `ms` ms via `setInterval`. Never completes on its own.

```ts
intervalSource<MS extends number>(ms: MS): IntervalSource<MS>
```

---

### `timeoutSource(ms, value?)` — Replayable

Emits a single value after `ms` ms via `setTimeout`, then completes.

```ts
timeoutSource<MS extends number, VALUE = void>(ms: MS, value?: VALUE): TimeoutSource<MS, VALUE>
```

---

### `eventTargetSource(target, eventType)`

Emits DOM events from an `EventTarget`. New listener per consumer, removed on termination.

```ts
eventTargetSource<EVENT_TYPE>(target: EventTarget, eventType: EVENT_TYPE): EventTargetSource<EVENT_TYPE>
```

---

### `abortSignalSource(signal)`

Emits once when the `AbortSignal` is aborted, then completes. Emits immediately if already aborted.

```ts
abortSignalSource(signal: AbortSignal): AbortSignalSource
```

---

### `abortControllerSource(controller)`

Convenience wrapper around `abortSignalSource` accepting an `AbortController`.

```ts
abortControllerSource(controller: AbortController): AbortControllerSource
```

---

### `gcTokenSource(token)`

Emits once when the object is garbage collected via `FinalizationRegistry`.

```ts
gcTokenSource(token: object): GCTokenSource
```

---

### `mergeSource(...sources)`

Merges multiple sources into one. Terminates when the **last** input terminates.

```ts
mergeSource<INPUTS extends Consumable[]>(...$inputs: INPUTS): MergeSource<INPUTS>
```

---

### `combineSource(...sources)`

Emits a tuple of latest values whenever any input emits. Uses `EMPTY` for inputs not yet emitted. Terminates when **any** input terminates.

```ts
combineSource<INPUTS extends Consumable[]>(...$inputs: INPUTS): CombineSource<INPUTS>
```

**Side channel:** none (use `latest(1)` on each input for `combineLatest` semantics)

---

### `zipSource(...sources)`

Emits a tuple only when all inputs have provided a new value. Terminates when **any** input terminates.

```ts
zipSource<INPUTS extends Consumable[]>(...$inputs: INPUTS): ZipSource<INPUTS>
```

| Property | Description                                               |
| -------- | --------------------------------------------------------- |
| `$rest`  | Source emitting unmatched values remaining at termination |

---

## Transformers

All transformers are functions that accept a source via `.pipe()` and return a new source.

---

### Filtering

#### `filter(predicate, complement?)`

Passes only values satisfying the predicate. Rejected values go to `.$complements` (lazy, passive).

```ts
filter<INPUT, VALUE, FILTERED extends VALUE>(
  predicate: (value: VALUE, index: number) => boolean,
  complement?: (value: VALUE, index: number) => void
)
```

| Property        | Description                        |
| --------------- | ---------------------------------- |
| `.$complements` | `Source<VALUE>` of rejected values |

```ts
const f = valuesSource(1, 2, 3, 4).pipe(filter((v) => v % 2 === 0));
f.$complements.pipe(listen(console.log)); // 1, 3
f.pipe(listen(console.log)); // 2, 4
```

---

#### `take(n)`

Takes the first `n` values then terminates.

```ts
take(n: number)
```

---

#### `takeWhile(predicate)`

Takes values while predicate returns `true`. Terminates on first `false`.

```ts
takeWhile<VALUE>(predicate: (value: VALUE) => boolean)
```

---

#### `takeUntil($notifier)`

Takes values until the notifier emits, then terminates.

```ts
takeUntil($notifier: Consumable)
```

---

#### `skip(n)`

Skips the first `n` values, then passes all through.

```ts
skip(n: number)
```

---

#### `skipWhile(predicate)`

Skips values while predicate returns `true`. Passes all once it returns `false`.

```ts
skipWhile<VALUE>(predicate: (value: VALUE) => boolean)
```

---

#### `skipUntil($notifier)`

Skips all values until the notifier emits, then passes all through.

```ts
skipUntil($notifier: Consumable)
```

---

#### `distinct(keySelector?, $flushes?)`

Filters duplicates using a `Set`. Optional key selector and flush notifier.

```ts
distinct<VALUE, KEY>(keySelector?: (value: VALUE) => KEY, $flushes?: Consumable)
```

---

#### `first(predicate?)`

Emits the first value (or first matching predicate) then terminates. Emits `EMPTY` if source completes without a match.

```ts
first<VALUE>(predicate?: (value: VALUE, index: number) => boolean)
```

---

#### `last()`

Emits the last value when source completes. Emits `EMPTY` if source completes without emitting.

```ts
last();
```

---

#### `range(start, offset)`

Passes only values at indices `[start, start + offset]` (0-based). Terminates after `offset` values.

```ts
range(start: number, offset: number)
```

---

### Transformation

#### `map(fn)`

Transforms each value with `(value, index) => result`.

```ts
map<VALUE, MAPPED>(mapper: (value: VALUE, index: number) => MAPPED)
```

---

#### `flat(depth?)`

Flattens a stream of arrays. `depth` default `0` = one level (same as `Array.flat(1)`).

```ts
flat<DEPTH extends number>(depth?: DEPTH)
```

---

#### `flat$(depth?)`

Flattens a stream of `Consumable`s sequentially. Waits for each inner to complete before moving to the next.

```ts
flat$(depth?: number)
```

---

#### `scan(seed, reducer)`

Accumulates with a reducer, emitting the running accumulator after each value.

```ts
scan<ACC, VALUE>(acc: ACC, reducer: (acc: ACC, value: VALUE) => ACC)
```

---

#### `scanArray()`

Accumulates all values into a growing array (mutated in place). Use `last()` for the final array.

```ts
scanArray();
```

---

#### `resolve(concurrency?)`

Resolves a stream of `Promise`s. Emits resolved values or `Error<reason>` for rejections. Default concurrency `1`.

```ts
resolve(concurrency?: number)
```

---

#### `safe(fn)`

Wraps a transformer pipeline in try/catch. On error emits `Error<thrown>` instead of crashing.

```ts
safe<INPUT, OUTPUT>(fn: ($input: INPUT) => OUTPUT)
```

---

#### `context(ctx)`

Wraps each value with a shared mutable context: `{ value, context }`.

```ts
context<INPUT, CTX>(context: CTX)
// emits: { value: VALUE; context: CTX }
```

---

### Aggregation

All aggregation transformers emit a running value after each input. Pipe `last()` to get only the final result.

#### `count()`

Emits the running count.

```ts
count();
```

#### `sum()`

Emits the running sum (numeric source).

```ts
sum();
```

#### `min()`

Emits the running minimum (numeric source).

```ts
min();
```

#### `max()`

Emits the running maximum (numeric source).

```ts
max();
```

#### `every(predicate)`

Emits `true` while all values satisfy the predicate. Emits `false` and terminates on first failure.

```ts
every<VALUE>(predicate: (value: VALUE, index: number) => boolean)
```

#### `batch(size)`

Collects values into arrays of up to `size`. Emits partial batch on completion.

```ts
batch(size: number)
```

#### `buffer(size, startBufferEvery?)`

Sliding-window arrays of `size`. New buffer starts every `startBufferEvery` values (default = `size`).

```ts
buffer<SIZE extends number>(size: SIZE, startBufferEvery?: number)
```

#### `zip(...others)`

Combines values pairwise into tuples. Emits only when all inputs have a new value. Terminates when any input terminates.

```ts
zip<OTHERS extends Consumable[]>(...others: OTHERS)
```

| Property | Description                               |
| -------- | ----------------------------------------- |
| `.$rest` | Unmatched values remaining at termination |

#### `combine(...others)`

Emits a tuple of latest values whenever any input emits. Uses `EMPTY` for inputs not yet emitted.

```ts
combine<OTHERS extends Consumable[]>(...others: OTHERS)
```

#### `merge(...others)`

Merges multiple sources. Terminates when the **primary** input terminates.

```ts
merge<OTHERS extends Consumable[]>(...others: OTHERS)
```

---

### Flow Control

#### `debounce(ms)`

Emits the latest value only after `ms` ms of silence. Each new value resets the timer.

```ts
debounce(ms: number)
```

---

#### `delay(ms)`

Delays each value by `ms` ms (`setTimeout`). Uses `queueMicrotask` if `ms <= 0`.

```ts
delay(ms?: number)
```

---

#### `delayOnce(ms)`

Delays only the first value by `ms` ms. All subsequent values pass through immediately.

```ts
delayOnce(ms: number)
```

---

#### `pace(ms)`

Rate-limits to at most one value per `ms` ms. Values arriving after the window pass through immediately; values arriving before are scheduled for the next slot. Credit flows back to slow the upstream.

```ts
pace(ms: number)
```

---

#### `gate($control)`

Opens/closes the stream based on a boolean control source. `true` opens, `false` closes.

```ts
gate(control: Consumable<boolean>)
```

---

#### `passive()`

Subscribes without calling `next()`. Never drives upstream pulls. Use to observe a shared stream without affecting throughput.

```ts
passive();
```

---

#### `pump()`

Eagerly drains the upstream by pre-granting credit, pushing all values into an internal `Stream`.

```ts
pump();
```

---

#### `share()`

Converts a pull-based source into a single shared multicast execution via `Stream.from`.

```ts
share();
```

---

#### `latest(n)`

Buffers the last `n` values. Late consumers receive buffered values immediately on subscription. Requires `share()` before it for replayable sources.

```ts
latest(n: number)
```

---

#### `switch$()`

Subscribes to the latest inner `Consumable` emitted by upstream, cancelling the previous one. Non-`Consumable` values pass through directly.

```ts
switch$();
```

---

### Side Effects & Termination

#### `tap(fn)`

Runs a side-effect `(value, index) => void` for each value. Value passes through unchanged.

```ts
tap<VALUE>(fn: (value: VALUE, index: number) => void)
```

---

#### `tapInput(fn)`

Runs a side-effect with access to the raw input source before it enters the pipeline. Returns input unchanged.

```ts
tapInput<INPUT>(fn: ($input: INPUT) => void)
```

---

#### `tapTerminate(fn)`

Runs a callback when the stream terminates, receiving the termination reason. Values pass through unchanged.

```ts
tapTerminate(fn: (reason: TerminateReason) => void)
```

---

#### `listen(fn?)`

Hot driver — consumes the source, calls `fn` for each value, auto-grants credit. Returns the input source for chaining. Callback is optional.

```ts
listen<VALUE>(callback?: (value: VALUE) => void)
```

---

#### `terminate()`

Ignores all values. Emits the termination reason (`"complete"` or `"abort"`) as a single value when the stream ends.

```ts
terminate();
```

---

#### `scope(...others)`

Terminates the pipeline when **any** of the notifier sources terminates.

```ts
scope(...others: [Consumable, ...Consumable[]])
```

---

#### `scopeStrict(...others)`

Like `scope`, but requires **all** notifiers to terminate before the pipeline terminates.

```ts
scopeStrict(...others: [Consumable, ...Consumable[]])
```

---

#### `print(label?)`

Logs each value to the console with an optional label prefix. Value passes through unchanged.

```ts
print(label?: string)
```

---

#### `toConsole(label?)`

Prints each value to the console and eagerly drains the stream via `pump()`. Terminal operator.

```ts
toConsole(label?: string)
```
