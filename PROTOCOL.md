# Execution Gating Stream Protocol (EGSP)

The **Execution Gating Stream Protocol (EGSP)** is a structural and behavioral architectural contract for handling data streams from first principles. Unlike traditional reactive streaming protocols that enforce bidirectional negotiation (where the consumer forces the producer to slow down), EGSP establishes an **Inward-Facing Isolation Barrier** within the consumer layer itself.

Under this protocol, upstream data sources are free to emit payloads at their own native velocities. Backpressure is not enforced by choking the data source, but by strictly gating the **execution context** of the processing handler.

---

## 1. Core Architectural Philosophy

EGSP splits the streaming lifecycle into two entirely decoupled vectors:

1. **Unconstrained Ingestion (Push Phase):** The upstream environment can push data at any rate, completely blind to downstream bottlenecks. The consumer acts as a structural "shock absorber."
2. **Gated Execution (Processing Phase):** The processing handler is completely insulated from the push rate. It is legally forbidden from running unless the consumer has explicitly allocated internal execution authority (Credit).

```text
  Upstream Source (Hot or Cold)
         │
         │ (High-Velocity Pushes: push / pushMany / pushBatch)
         ▼
  ┌────────────────────────────────────────────────────────┐
  │ Consumer (Isolation Barrier)                           │
  │                                                         │
  │              ┌──────────────┐                          │
  │  Inflow ────►│ _credit > 0? ├───[Yes]──► Execute       │
  │              └──────┬───────┘            Handler       │
  │                     │                                  │
  │                    [No]                                │
  │                     │                                  │
  │                     ▼                                  │
  │              ┌──────────────┐                          │
  │              │  FIFO Queue  │                          │
  │              └──────────────┘                          │
  └────────────────────────────────────────────────────────┘
```

---

## 2. Structural Contracts

### 2.1 The Consumable Interface

A `Consumable` represents a state-free capability provider. It is **not a producer**. It is a gateway that accepts a processing contract and configuration, returning an active execution coordinator.

```typescript
export interface Consumable<VALUE> {
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
}
```

### 2.2 The Consumer Coordinator

The `Consumer` is the structural boundary where backpressure lives. It maintains an internal credit counter and an optional FIFO storage buffer to manage the decoupling of ingestion from execution.

---

## 3. The `push` Ingestion Engine (The Shock Absorber)

The `push` operation is where the core complexity of the protocol is managed. When a payload is delivered, the Consumer executes a deterministic routing check:

- **Condition A (Zero-Allocation Inline Pass):** If internal credit is available and no previous items are backlogged, the payload bypasses the queue completely. It is immediately processed inline, and credit is decremented.
- **Condition B (Saturated Demand):** If credit is `0` or a backlog exists, the payload is diverted into a FIFO queue. The upstream execution path returns instantly, preventing thread-blocking at the source.

### Inflow Routing Logic

```typescript
if (this._credit > 0 && !this._queue?.size) {
  this._handler(this, value);
  this._credit--;
} else {
  this.getOrCreateQueue().enqueue(value);
}
```

---

## 4. Gated Execution Mechanics (`next`)

Execution of the user-defined handler is strictly metered. A single credit represents explicit authorization to process exactly **one** payload, ensuring the stream moves at the consumer's native processing rate.

- **Single-Unit Step Execution:** Invoking `.next()` grants one credit. If the FIFO queue contains backlogged items, the consumer processes exactly **one** item from the queue, executes the handler, and immediately decrements the credit back to `0`.
- **Pace Control:** The consumer does not automatically flush the remaining queue. It pauses execution after that single item, waiting until the handler context explicitly calls `.next()` again to pull the subsequent item.
- **Credit Accumulation:** If `.next()` is invoked while the queue is completely empty, the credit is stored in memory (`_credit++`) to immediately authorize the very next arriving payload without enqueuing it.

---

## 5. Stream Elasticity Configurations

Because the consumer is the home of backpressure, the upstream source does not need to know about the credit system. However, the protocol accommodates different streaming profiles via an optional feedback circuit:

### Hot / Forced Streams (e.g., UI Events, Network Sockets)

The upstream source cannot slow down and has no concept of demand. It continuously fires data into `push`.

- **Equilibrium/Fast Consumer:** If the consumer's execution pace is equal to or faster than the influx, **no queue is involved**. Payloads pass directly into the handler layout with zero overhead.
- **Overpace Protection:** If the source bursts or outpaces the handler, the queue instantiates on-demand as a temporary shock absorber until the consumer catches up.

### Cold / Lazy Streams (e.g., File System Chunks, DB Cursors)

The upstream source is elastic and can generate data on demand. It provides an `options.next` hook. When the consumer processes its buffer and realizes it is out of data, it fires `options.next(this)`. This acts as a downstream-to-upstream bridge, signaling the lazy source that it is safe to harvest and push the next batch of payloads.

---

## 6. Deterministic Lifecycle States & Neutralization

The protocol enforces a precise state machine for winding down execution. Transitioning out of the `active` state occurs via two distinct terminal paths: a graceful wind-down (`complete`) or an immediate destruction (`abort`).

```text
                        ┌──────────┐
                        │  Active  │
                        └────┬─────┘
                             │
               ┌─────────────┴─────────────┐
               │ terminate("complete")     │ terminate("abort")
               ▼                           ▼
         ┌───────────┐               ┌───────────┐
         │   Drain   │               │   Abort   │
         └─────┬─────┘               └───────────┘
               │                           ▲
         ┌─────┴─────────────────────┐     │
         │                           │     │ terminate("abort")
         ▼ (Queue Empties)           ▼     │ (Emergency Stop)
   ┌───────────┐               ┌───────────┘
   │ Complete  │               │
   └───────────┘               └────────────────────────────
```

### 6.1 The Graceful Wind-Down Path (`complete` / `drain`)

When `terminate("complete")` is invoked while the backlog contains items, the consumer transitions into a temporary `drain` phase designed to clear queued payloads safely:

1. **Inflow Ingestion Stubbing:** The `push` method is instantly replaced with a static no-op (`EMPTY_THIS_FUNCTION`). The consumer is now closed to new external data.
2. **Execution Reservation:** The `.next()` and `.terminate()` methods remain fully functional.
3. **Paced Evacuation:** The consumer continues to invoke `.next()` to pull and execute the remaining items in the queue one by one at its own deliberate consumption rate.
4. **Natural Finalization:** Once the queue size drops to zero during this phase, the engine automatically crosses the threshold into the definitive `complete` state, where all methods are stubbed and memory is wiped clean.

### 6.2 The Immediate Emergency Stop & Interruption Path (`abort`)

If `terminate("abort")` is called—either from the initial `active` state or as an emergency interruption **during the `drain` phase**—the protocol triggers an absolute, destructive stop:

1. **Total Method Stubbing:** All core control pathways (`push`, `next`, `terminate`) are simultaneously replaced with static no-ops (`EMPTY_THIS_FUNCTION` / `EMPTY_FUNCTION`).
2. **Immediate Queue Evacuation:** The internal queue is instantly cleared (`_queue.clear()`), and its references are shattered (`undefined`), dumping all unexecuted data for instant garbage collection.
3. **Resource Cleansing:** The user-defined handler and option hooks are completely unlinked, ensuring no late-arriving asynchronous ticks can cause memory leaks or post-mortem side effects.
