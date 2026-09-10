# Execution Gating Stream Protocol (EGSP)

The **Execution Gating Stream Protocol (EGSP)** is a structural and behavioral architectural contract for handling data streams from first principles. Unlike traditional reactive streaming protocols that enforce bidirectional negotiation (where the consumer begs the producer to slow down), EGSP establishes an **Inward-Facing Isolation Barrier** within the consumer layer itself.

Under this protocol, upstream data sources are free to emit payloads at their own native velocities. Backpressure is not enforced by choking the data source, but by strictly gating the **execution context** of the processing handler.

---

## 1. Core Architectural Philosophy

EGSP splits the streaming lifecycle into two entirely decoupled vectors:

1. **Unconstrained Ingestion (Push Phase):** The upstream environment can push data at any rate, completely blind to downstream bottlenecks. The consumer acts as a structural "shock absorber."
2. **Gated Execution (Processing Phase):** The processing handler is completely insulated from the push rate. It is legally forbidden from running unless the consumer has explicitly allocated internal execution authority (Credit).

```git

  Upstream Source (Hot or Cold)
         │
         │ (High-Velocity Pushes: push / pushMany / pushBatch)
         ▼
  ┌────────────────────────────────────────────────────────┐
  │ Consumer (Isolation Barrier)                           │
  │                                                        │
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

The `Consumer` is the structural boundary where backpressure lives. It maintains an internal credit counter and a FIFO storage buffer to manage the decoupling of ingestion from execution.

---

## 3. The `push` Ingestion Engine (The Shock Absorber)

The `push` operation is where the core complexity of the protocol is managed. When a payload is delivered, the Consumer must execute a deterministic routing routing check:

- **Condition A (Available Demand):** If internal credit is greater than `0` and no previous items are backlogged, the payload is immediately processed inline. Credit is decremented by 1.
- **Condition B (Saturated Demand):** If credit is `0` or a backlog exists, the payload is immediately diverted into a FIFO queue. The upstream execution path returns instantly, preventing thread-blocking at the source.

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

Execution of the user-defined handler is entirely pull-based from the perspective of the handler loop. Credit represents explicit authorization to process exactly **one** payload.

- **Granting Credit:** Invoking `.next()` increments the internal credit balance.
- **Draining the Backlog:** If the FIFO queue contains items, granting a credit instantly shifts the consumer into a drain loop, pulling items out of the queue and processing them until credits hit `0` or the queue empties.
- **Idling:** If `.next()` is called while the queue is empty, the credit is stored in memory to authorize the _next_ arriving push item.

---

## 5. Optional Upstream Elasticity (`options.next`)

Because the consumer is the home of backpressure, the upstream source does not need to know about the credit system. However, the protocol accommodates two types of data streams via an optional feedback circuit:

### Hot / Forced Streams (e.g., UI Events, Network Sockets)

The upstream source cannot slow down and has no concept of demand. It ignores the consumer's state and continuously fires data into `push`. The consumer relies entirely on its internal FIFO queue to maintain backpressure at the handler level.

### Cold / Lazy Streams (e.g., File System Chunks, DB Cursors)

The upstream source is elastic and can generate data on demand. It provides an `options.next` hook. When the consumer processes its buffer and realizes it is out of data, it fires `options.next(this)`. This acts as a downstream-to-upstream bridge, signaling the lazy source that it is safe to harvest and push the next batch of payloads.

---

## 6. Deterministic Neutralization

To prevent memory leaks and late asynchronous side-effects in long-lived architectures, the protocol enforces a strict **Terminal Phase**. Once a consumer changes state to `complete` or `abort`, the instance must be entirely neutralized:

1. **Method Stubbing:** Active routing operations (`push`, `next`, `terminate`) are immediately replaced with inert, static no-op functions (`EMPTY_THIS_FUNCTION`).
2. **State Cleansing:** The user-defined `Handler` reference is severed and pointed to an empty function (`EMPTY_FUNCTION`).
3. **Queue Evacuation:** The internal queue reference is destroyed, instantly making any backlogged payloads eligible for garbage collection.

Any payload pushed to a neutralized consumer vanishes safely without executing logic, preserving system stability.
