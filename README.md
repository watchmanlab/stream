# @soffinal/stream

> Multi-paradigm reactive primitives built on a **pull-on-push** protocol.

A TypeScript reactive library that solves backpressure, async workflows, and event composition without making you think about any of that until you need to.

Zero runtime dependencies. Full TypeScript inference. Works in the browser, Node, Bun, and Deno.

---

## Install

```sh
# npm
npm install @soffinal/stream

# JSR
jsr add @soffinal/stream
```

---

## Start Here

You have a button. You want to do something when it's clicked.

```typescript
import { fromEventTarget } from "@soffinal/stream/sources";
import { listen } from "@soffinal/stream/transformers";

fromEventTarget(button, "click").pipe(listen((e) => console.log("clicked!")));
```

Now you want to debounce it — only react after the user stops clicking for 300ms.

```typescript
import { fromEventTarget } from "@soffinal/stream/sources";
import { debounce, listen } from "@soffinal/stream/transformers";

fromEventTarget(button, "click")
  .pipe(debounce(300))
  .pipe(listen((e) => console.log("settled")));
```

Now you want to fetch something on each click, but only keep the latest request — if a new click comes in before the previous fetch finishes, cancel it.

```typescript
import { fromEventTarget } from "@soffinal/stream/sources";
import { debounce, map, switch$, resolve, listen } from "@soffinal/stream/transformers";

fromEventTarget(searchInput, "input")
  .pipe(debounce(300))
  .pipe(map((e) => fetch(`/api/search?q=${e.target.value}`)))
  .pipe(resolve())
  .pipe(listen((results) => render(results)));
```

That's the pattern. You start with a source, chain transformers with `.pipe()`, and end with `listen`. Each step is independent and composable.

---

## How It Works (The Short Version)

Most reactive libraries have a hidden problem: the producer controls the rate. If your consumer is slow — doing async work, rendering, writing to a database — values pile up and memory grows.

This library solves that with a **credit system**. A consumer only processes a value when it has credit. You grant credit by calling `next()` inside your handler. If no credit is available, values queue up and drain at the consumer's own pace.

```typescript
import { Consumer } from "@soffinal/stream";

const consumer$ = new Consumer<number>((self$, value) => {
  console.log(value);
  self$.next(); // "I'm ready for the next one"
});

consumer$.next(); // grant initial credit
consumer$.push(1); // fires immediately
consumer$.push(2); // fires immediately (self$.next() re-granted credit)
```

You rarely use `Consumer` directly — transformers and `listen` handle it for you. But this is what's happening under the hood in every pipeline.

### Reentrancy

Because the protocol is synchronous and callback-based, reentrancy is a real concern — what if `next()` is called from inside the handler before it returns? The credit counter handles it without any extra machinery. The drain loop only runs when `credit === 1`. If `next()` is called re-entrantly, credit increments to 2 and the loop guard exits immediately — the outer loop picks up the extra credit on its next iteration. No recursion, no locks.

### Pipelines Are Transducers

Transformers like `map` and `filter` don't create a second consumer and push into it. They consume from the input and return _that same consumer_ with the handler wrapped:

```typescript
// map's entire consume implementation
consume(handler, options) {
  return this.$input.consume(
    (c$, v) => handler(c$, this.mapper(v)),
    options
  );
}
```

Chain several transformers and the whole pipeline collapses into nested function calls — one consumer, no intermediate allocations, fully inlineable by the JIT. This is the transducer pattern, for free, on every linear pipeline. It only breaks when you introduce `share()`, which needs a real multicast boundary.

This is also why many transformers that _could_ be expressed in terms of others are written from scratch. `range` could be `skip(start).pipe(take(offset))` — clean and obvious. But that creates two consumers, two handler wrappers, two objects. Written directly against the input, it's one consumer, one handler, zero intermediate allocations. The composability is a user-facing property. The implementation doesn't have to pay for it.

---

## The Three Primitives

Everything in this library is built from three concepts:

**`Stream` / `Source` / `Consumable`** — one primitive, three layers. `Consumable` is the interface: anything with a `consume` method participates in the pipeline. `Source` is the abstract base that adds `pipe` and `for await...of`. `Stream` is the concrete multicast implementation. From the user's perspective they are interchangeable — if it has `consume`, it's in the pipeline.

> **Naming convention:** any variable holding a `Consumable` is prefixed with `$`. Any variable holding a `Consumer` (the object returned by `.consume()`) is suffixed with `$`, the suffix is applied on higher order transformers like `switch$`,`flat$` . So `$search` is something you can consume from, and `search$` is the consumer you got back from consuming it. This convention is used consistently throughout the library's own source code — every transformer, every source, every internal variable follows it. When you read the source of any transformer, you instantly know which variables are producers and which are consumers without reading a single comment. It's worth adopting in your own code for the same reason.

**`pipe`** — the composition mechanism. `source.pipe(fn)` is literally `fn(source)`. Transformers are not special — they are plain `Source`s whose constructor accepts another `Consumable` as input. `map`, `filter`, `debounce` are all just `Source` subclasses. `pipe` is the glue that passes one into the next.

> **Transformers are classes.** The function you call in `.pipe()` is just a factory that returns an instance. Because they're classes with a fixed shape, the JIT can optimize them aggressively. And because they're classes, you can instantiate them directly — useful when you want to build a pipeline step by step, hold a reference to an intermediate stage, or add transformers conditionally:
>
> ```typescript
> // These are identical
> $source.pipe(map((v) => v * 2));
> new Map($source, (v) => v * 2);
>
> // Step-by-step — useful for conditional pipelines
> const $filtered = new Filter($source, isValid);
> const $mapped = new Map($filtered, transform);
> $mapped.pipe(listen(console.log));
> ```

---

## Real Use Cases

### Search with debounce and cancellation

```typescript
fromEventTarget(searchInput, "input")
  .pipe(map((e) => e.target.value))
  .pipe(debounce(300))
  .pipe(map((query) => fetch(`/api/search?q=${query}`)))
  .pipe(resolve()) // resolves the promise, emits the result
  .pipe(listen(renderResults));
```

### Polling with a stop condition

```typescript
import { fromInterval } from "@soffinal/stream/sources";
import { map, resolve, takeUntil, listen } from "@soffinal/stream/transformers";
import { fromTimeout } from "@soffinal/stream/sources";

fromInterval(5000)
  .pipe(map(() => fetch("/api/status")))
  .pipe(resolve())
  .pipe(takeUntil(fromTimeout(60_000))) // stop after 60 seconds
  .pipe(listen((status) => updateUI(status)));
```

### Sharing a source across multiple consumers

```typescript
import { fromInterval } from "@soffinal/stream/sources";
import { share, listen, passive } from "@soffinal/stream/transformers";

const $ticker = fromInterval(1000).pipe(share());

// This drives the interval
$ticker.pipe(listen((v) => updateClock(v)));

// This observes without driving — won't start the interval on its own
$ticker.pipe(passive()).pipe(listen((v) => logTick(v)));
```

> **Why `passive()`?** By default every consumer drives the source. The first consumer to call `next()` triggers production, and the value is broadcast to all consumers — any that already have credit fire immediately, no buffering involved. If you add a logger or analytics consumer, you don't want it to trigger a network request or start a timer just because it subscribed. `passive()` opts that consumer out of driving entirely.

### Fetching multiple URLs with concurrency control

```typescript
import { fromGenerator } from "@soffinal/stream/sources";
import { resolve, listen } from "@soffinal/stream/transformers";

fromGenerator(function* () {
  yield fetch("/api/users");
  yield fetch("/api/posts");
  yield fetch("/api/comments");
})
  .pipe(resolve(2)) // max 2 in-flight at once
  .pipe(listen(console.log));
```

> **Why `resolve(n)` instead of async map?** Promises are just values here. The core doesn't know what a promise is. `resolve` is a transformer that handles promise resolution with explicit concurrency control. You decide the concurrency — it's not hidden inside an operator.

### Buffering late subscribers

```typescript
const $prices = fromEventTarget(socket, "message")
  .pipe(map((e) => JSON.parse(e.data)))
  .pipe(share())
  .pipe(latest(5)); // buffer last 5 values

// A component that mounts later immediately gets the last 5 prices
$prices.pipe(listen(updatePriceChart));
```

### Sliding windows and batching

```typescript
// Emit arrays of the last 3 values, starting a new window every value
fromInterval(100).pipe(buffer(3, 1)).pipe(listen(console.log)); // [0,1,2], [1,2,3], [2,3,4], ...

// Collect into non-overlapping chunks of 10
fromInterval(100).pipe(batch(10)).pipe(listen(processBatch)); // [0..9], [10..19], ...
```

### Cancelling the previous request with `switch$`

```typescript
const $outer = new Stream<Consumable<Response>>();

$outer
  .pipe(switch$()) // cancels previous inner stream when a new one arrives
  .pipe(listen(handleResponse));

$outer.push(of(fetch("/api/search?q=a")));
$outer.push(of(fetch("/api/search?q=ab"))); // previous fetch is cancelled
```

> **`switch$` gotcha:** it does not work with replayable sources like `of(of(1,2), of(3,4))`. Because `switch$` is eager on the outer stream but lazy on the inner one, a replayable outer source completes synchronously before `switch$` can attach a consumer to the last inner value. Always use `switch$` with hot sources — `Stream`, `fromEventTarget`, etc.

### Gate: open and close a stream

```typescript
const $isOnline = fromEventTarget(window, "online")
  .pipe(map(() => true))
  .pipe(merge(fromEventTarget(window, "offline").pipe(map(() => false))));

// Only emit sensor data while online
fromInterval(100).pipe(gate(isOnline$)).pipe(listen(sendToServer));
```

### Error handling

```typescript
import { safe, filter, listen } from "@soffinal/stream/transformers";
import { Error } from "@soffinal/stream";

of(1, 2, 3)
  .pipe(
    safe(
      map((v) => {
        if (v === 2) throw new TypeError("bad value");
        return v;
      }),
    ),
  )
  .pipe(
    listen((v) => {
      if (v instanceof Error) {
        console.error("caught:", v.value);
      } else {
        console.log(v); // 1, 3
      }
    }),
  );
```

> **Why are errors values?** Because most of your code doesn't throw. If errors were baked into every operator, you'd pay the cost of error-awareness on every single value, in every pipeline, for a case that might never happen. With `safe`, you opt in exactly where you need it. Errors flow through the same channel as values — you can filter them, log them, retry, or ignore them.

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

---

## Streams

### `Stream<VALUE>`

A multicast push source. Push values in, all consumers receive them. Exposes lifecycle as first-class observable sources.

```typescript
const $stream = new Stream<number>();

$stream.$firstConsumerJoin.pipe(listen(() => console.log("first consumer joined")));
$stream.$lastConsumerLeft.pipe(listen(() => console.log("last consumer left")));
$stream.$terminate.pipe(listen((r) => console.log("terminated:", r)));

$stream
  .consume((c$, v) => {
    console.log(v);
    c$.next();
  })
  .next();
$stream.push(1).push(2).push(3);
$stream.terminate("complete");
```

**Lifecycle events:** `$push`, `$next`, `$consumerJoin`, `$consumerLeft`, `$firstConsumerJoin`, `$lastConsumerLeft`, `$drain`, `$terminate`

### `Signal<VALUE>`

A `Stream` that auto-terminates after the first push. For one-shot events.

```typescript
const $ready = new Signal<void>();
$ready.pipe(listen(() => console.log("ready!")));
$ready.push(); // emits, then terminates
```

### `State<VALUE>`

A `Stream` with a `.value` property. Setting it pushes to all consumers.

```typescript
const $count = state(0);
$count.pipe(listen((v) => console.log("count:", v)));
$count.value = 1; // logs 'count: 1'
$count.value = 2; // logs 'count: 2'
```

---

## Transformers

### Filtering & Slicing

```typescript
filter(predicate)         // pass values matching predicate; rejected values go to .$complements
take(n)                   // first n values then terminate
skip(n)                   // skip first n values
range(start, offset)      // skip `start` values, then pass `offset` values (slice by index position)
takeWhile(predicate)      // take while predicate is true
takeUntil(notifier)       // take until notifier emits
skipWhile(predicate)      // skip while predicate is true
skipUntil(notifier)       // skip until notifier emits
first(predicate?)         // first value (or first matching predicate)
last()                    // last value on completion
distinct(keySelector?, $flushes?) // deduplicate using a Set across ALL seen values, not just consecutive ones
```

### Transformation

```typescript
map(fn); // transform each value
tap(fn); // side effect, passes value through
tapInput(fn); // tap on the input consumer
tapTerminate(fn); // tap on termination
scan(seed, reducer); // running accumulator, emits each step
scanArray(); // accumulate all values into a single growing array, emit same reference each step
context(ctx); // wrap each value as { value, context }
```

### Flattening

```typescript
flat(depth?)              // flatten arrays
flat$()                   // flatten inner Consumables sequentially — waits for each to complete before next
switch$()                 // switch to latest inner Consumable, cancel previous — use with hot sources only
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
buffer(size, every?)      // sliding window arrays
batch(size)               // non-overlapping chunks
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
resolve(concurrency?)     // resolve Promises, emit values or Error<reason>
```

### Error Handling

```typescript
safe(fn); // wrap pipeline in try/catch, emit Error<thrown> on throw
```

### Lifecycle

```typescript
terminate(); // emit termination reason, ignore values
scope(...notifiers); // terminate when ANY notifier terminates
scopeStrict(...notifiers); // terminate only when ALL notifiers have terminated
```

### Aggregation

All aggregation operators emit **running values**. Pipe `last()` after them to get the final result.

```typescript
sum(); // running sum
min(); // running minimum
max(); // running maximum
count(); // running count
every(predicate); // true if all values match
range(); // [min, max] running range
```

```typescript
of(1, 2, 3, 4, 5)
  .pipe(scan(0, (acc, v) => acc + v))
  .pipe(last())
  .pipe(listen(console.log)); // 15
```

---

## Termination

Every stream and consumer has two termination modes:

- `"abort"` — stop immediately, clear all queues
- `"complete"` — drain remaining queued values at the consumer's own pace, then stop

Both work with the `using` / `await using` keywords:

```typescript
{
  using $stream = new Stream<number>(); // abort on block exit
}

{
  await using $stream = new Stream<number>(); // complete on block exit, waits for drain
}
```

You can also scope a pipeline to a lifetime:

```typescript
// Automatically terminates when the component unmounts
const $unmount = new Signal<void>();

fromEventTarget(window, "resize").pipe(scope($unmount)).pipe(listen(handleResize));

// later...
$unmount.push(); // everything cleans up
```

### Filter complements, zip rest, and side channels

`filter` doesn't just pass or drop values. Rejected values go to a lazy `$complements` stream at zero cost if unused:

```typescript
const $even = of(1, 2, 3, 4, 5).pipe(filter((v) => v % 2 === 0));
$even.pipe(listen(console.log)); // 2, 4
$even.$complements.pipe(listen(console.log)); // 1, 3, 5
```

Use `tapInput` to access these side-channel properties mid-pipe without breaking the chain:

```typescript
$source
  .pipe(tapInput((f: Filter<...>) => f.$complements.pipe(listen(logRejected))))
  .pipe(filter(isValid))
  .pipe(listen(process));
```

`zip` similarly exposes a `$rest` stream for unmatched values when one source ends before the other.

### `flat` vs `flat$` vs `switch$` — three flattening strategies

All three flatten a stream of streams, but they have completely different behaviors:

- `flat(depth?)` — flattens **arrays** into individual values. Nothing to do with inner streams.
- `flat$()` — flattens inner **Consumables sequentially**. Subscribes to the first, waits for it to complete, then subscribes to the next. Use when order matters and you need each inner stream to finish before the next starts.
- `switch$()` — always subscribes to the **latest** inner Consumable, cancelling the previous one. Use when only the most recent matters — like cancelling a previous search request when a new one arrives.

```typescript
// flat$ — sequential, ordered
of(of(1, 2), of(3, 4)).pipe(flat$()).pipe(listen(console.log)); // 1, 2, 3, 4 in order

// switch$ — latest wins, previous cancelled
const $outer = new Stream<Consumable<number>>();
$outer.pipe(switch$()).pipe(listen(console.log));
const $s1 = new Stream<number>();
const $s2 = new Stream<number>();
$outer.push(s1);
$s1.push(1); // logs 1
$outer.push(s2); // $s1 is cancelled
$s1.push(99); // ignored
$s2.push(2); // logs 2
```

### `combine` vs `zip` vs `combineLatest`

- `zip` — waits for all inputs to have a new value, emits a tuple
- `combine` — emits on any input using the last known value (or `EMPTY`) for the others
- `combineLatest` — pipe `latest(1)` to each input before `zip`:

```typescript
$s1
  .pipe(latest(1))
  .pipe(zip($s2.pipe(latest(1))))
  .pipe(listen(console.log));
```

### `distinct` — full deduplication with optional key and flush

`distinct` uses a `Set` across all seen values, not just consecutive ones. Accepts a `keySelector` and a `$flushes` notifier to reset the set:

```typescript
$stream.pipe(distinct((v) => v.id)).pipe(listen(console.log));
$stream.pipe(distinct((v) => v.id, pageChange$)).pipe(listen(console.log)); // reset on page change
```

### `pump` — eager drain

`pump` is a trigger. By default a pipeline is dormant — nothing runs until a downstream consumer calls `next()`. `pump` wakes it up immediately without waiting for any downstream subscriber.

Importantly, `pump` doesn't control the rate — the pipeline's own transformers do. If there's a `resolve(2)` upstream, values come through at the rate `resolve` allows. If there's a `debounce`, values come through at the debounced rate. `pump` just removes the requirement for a downstream consumer to exist before the pipeline starts.

This also means you don't need `listen` at the end. Use `tap` for side effects and `pump` to wake the pipeline — clean, self-contained, no terminal operator required:

```typescript
// tap + pump: side effects stay in the pipeline, pump is just the trigger
fromEventTarget(socket, "message")
  .pipe(map((e) => JSON.parse(e.data)))
  .pipe(resolve(2))
  .pipe(tap(updateUI)) // side effect lives here
  .pipe(pump()); // just wakes it up
```

Anything attached downstream of `pump()` is still dormant and needs its own wake-up call. You can use another `pump()`, or `tap + pump`, or `listen` — which is just a shorthand for `tap + pump`:

```typescript
const $hot = fromEventTarget(socket, "message")
  .pipe(map((e) => JSON.parse(e.data)))
  .pipe(resolve(2))
  .pipe(tap(updateUI))
  .pipe(pump()); // wakes the pipeline above

// these are all equivalent
$hot.pipe(listen(console.log)); // shorthand for tap + pump
$hot
  .pipe(map((v) => v.id))
  .pipe(tap(console.log))
  .pipe(pump()); // explicit tap + pump
```

### `context` — shared data that travels with the pipeline

Wraps each value as `{ value, context }` where the same context object is shared across all values. Its purpose goes far beyond accumulating state — it's a general-purpose carrier for anything that needs to be accessible at any downstream without threading it through the values themselves: session data, request metadata, correlation IDs, feature flags, user permissions, timing information.

```typescript
// Attach session metadata to every event
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

### `delay(0)` — microtask scheduling

`delay` with `ms <= 0` uses `queueMicrotask` instead of `setTimeout` — defers to the next microtask without leaving the current event loop tick.

### `State.value` vs `State.push`

Setting `.value` updates the stored value and notifies consumers. Calling `.push()` notifies consumers without updating `.value` — useful for transient events on a state stream:

```typescript
const $status = state<string>("idle");
$status.value = "loading"; // updates value, notifies consumers
$status.push("ping"); // notifies consumers, $status.value still === 'loading'
```

### Replayable sources

Sources like `of`, `fromIterable`, `fromGenerator`, `fromInterval`, `fromRange`, and `fromTimeout` are **replayable** — each consumer gets a fresh independent sequence. Use `share()` when you want multiple consumers to share the same running sequence instead of each replaying from the start.

**Critical gotcha with `share()` and synchronous replayable sources:** if the source is synchronous (like `of` or `fromIterable`) and the first consumer is also synchronous, it will drain the entire source in the same tick before any other consumer gets a chance to receive anything:

```typescript
const $shared = of(1, 2, 3).pipe(share());

$shared.pipe(listen(console.log)); // 1, 2, 3 — drains the source immediately
s$hared.pipe(listen(console.log)); // nothing — source already exhausted
```

This only affects sources that are both replayable **and** synchronous — `of`, `fromIterable`, `fromRange`, `fromGenerator` with a synchronous generator. `fromInterval` is replayable too but asynchronous — each consumer gets its own independent timer and the first consumer never drains it in the same tick, so sharing it is perfectly safe.

The fix is to register all consumers before any of them calls `next()`, or use `latest(n)` after `share()` to buffer values for late subscribers.

### `scope` vs `scopeStrict`

`scope` terminates the pipeline when **any** notifier terminates. `scopeStrict` waits until **all** notifiers have terminated.

```typescript
// scope — stops when the first of these fires
fromInterval(100).pipe(scope($userLogout, $sessionExpiry)).pipe(listen(tick));

// scopeStrict — keeps running until both tasks are done
fromInterval(500).pipe(scopeStrict($task1Done, $task2Done)).pipe(listen(checkProgress));
```

### `debounce` vs `pace`

Both slow down a stream but solve different problems.

`debounce` suppresses all values until the source goes quiet for `ms` milliseconds. Every new value resets the timer. Only the last value in a burst gets through. Use it when you want to react to the _end_ of activity — search inputs, resize handlers.

`pace` enforces a minimum gap between emissions. The first value fires immediately. If the next value arrives before the interval has elapsed, it's scheduled to fire when the gap is up. If it arrives _after_ the interval has already elapsed, it fires immediately — no artificial delay added. No values are dropped. This is what distinguishes it from `delay`, which always adds a fixed delay regardless of when the value arrived.

```typescript
// Only search after user stops typing for 300ms
fromEventTarget(input, "input").pipe(debounce(300)).pipe(listen(search));

// Process sensor data at most once every 500ms
fromEventTarget(sensor, "data").pipe(pace(500)).pipe(listen(process));
```

### The `EMPTY` sentinel

`EMPTY` is a special symbol (`Symbol.for('empty')`) used to represent the absence of a value. You encounter it in two places:

- `combine`: slots for inputs that haven't emitted yet are `EMPTY` rather than `undefined` (because `undefined` is a valid value a stream might emit)
- `first` and `last`: if the stream completes without emitting, or is aborted, these operators emit `EMPTY` so your downstream handler is always called

```typescript
import { EMPTY } from "@soffinal/stream";

of()
  .pipe(first())
  .pipe(
    listen((v) => {
      if (v === EMPTY) console.log("stream was empty");
      else console.log("got:", v);
    }),
  );
```

### `every` — short-circuits on first failure

`every` checks whether all values satisfy a predicate and terminates early the moment one fails — it does not wait for the stream to complete:

```typescript
of(1, -1, 3, 4)
  .pipe(every((v) => v > 0))
  .pipe(listen(console.log)); // false — stops after -1, never sees 3 or 4
```

### `range` transformer — slice by index

Not to be confused with `fromRange` (which generates integers). The `range` _transformer_ slices a stream by position — skip `start` values, then pass `offset` values:

```typescript
of("a", "b", "c$", "d", "e").pipe(range(1, 3)).pipe(listen(console.log)); // 'b', 'c$', 'd'
```

### `scanArray` — collect into a growing array

Accumulates all values into a single array, mutating and re-emitting the same reference each time. Use with `last()` to collect a stream into an array:

```typescript
of(1, 2, 3).pipe(scanArray()).pipe(last()).pipe(listen(console.log)); // [1, 2, 3]
```

### `terminate` transformer — observe the end

Ignores all values and emits only the termination reason (`'complete'` or `'abort'`) when the stream ends:

```typescript
$fetch.pipe(terminate()).pipe(
  listen((reason) => {
    if (reason === "complete") cleanup();
    if (reason === "abort") rollback();
  }),
);
```

### Value-changing transformers and detached pipelines

Some transformers fundamentally change the type of what flows through: `terminate` discards all values and emits a termination reason, `last` waits for completion and emits a single value, `every` short-circuits and emits a boolean. Piping through any of these changes the downstream type — you can't continue the original pipeline after them.

These are meant to be used on a detached branch. Two ways to do that:

Split at an intermediate stage and attach the side branch from there:

```typescript
const $results = source.pipe(map(transform)).pipe(resolve());

$results.pipe(listen(render)); // main pipeline
$results.pipe(terminate()).pipe(listen((reason) => cleanup(reason))); // side branch
$results.pipe(last()).pipe(listen((v) => log("final:", v))); // side branch
```

Or use `tapInput` to branch inline without breaking the fluent chain:

```typescript
$source
  .pipe(map(transform))
  .pipe(resolve())
  .pipe(
    tapInput(($r) => {
      $r.pipe(terminate()).pipe(listen((reason) => cleanup(reason)));
      $r.pipe(last()).pipe(listen((v) => log("final:", v)));
    }),
  )
  .pipe(listen(render)); // main pipeline continues unaffected
```

The split-variable style is clearer when the side branch is substantial. `tapInput` is cleaner when you want to keep everything in one fluent expression.

### `merge` termination behavior

`merge` terminates when the **primary** input (the one you piped from) terminates — not when all inputs terminate. Secondary inputs are terminated at that point too:

```typescript
const $s1 = new Stream<number>();
const $s2 = new Stream<number>();

$s1.pipe(merge($s2)).pipe(listen(console.log));

$s1.push(1); // logs 1
$s2.push(2); // logs 2
$s1.terminate("complete"); // pipeline ends, $s2 is also terminated
$s2.push(3); // never arrives
```

---

## `for await...of`

Every `Source` is an `AsyncIterable`. You can consume any pipeline with a standard `for await...of` loop.

```typescript
for await (const value of fromIterable([1, 2, 3]).pipe(map((v) => v * 2))) {
  console.log(value); // 2, 4, 6
}
```

---

## License

MIT
