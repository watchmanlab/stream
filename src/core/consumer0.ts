import { Queue, TerminateReason } from "./types";

// 1. Define the raw data state interface
export interface Consumer<VALUE> {
  handler: Consumer.Handler<VALUE>;
  options?: Consumer.Options<VALUE>;
  status: Consumer.Status;
  queue?: Queue<VALUE>;
  credit: number;
  initCleanup?: Consumer.InitCleanup;
}

// 2. Factory function (Replaces the constructor)
export function createConsumer<VALUE>(
  handler: Consumer.Handler<VALUE>,
  options?: Consumer.Options<VALUE>,
): Consumer<VALUE> {
  const consumer: Consumer<VALUE> = {
    handler,
    options: { ...options, next: options?.passive ? undefined : options?.next },
    status: "active",
    credit: 0,
  };

  consumer.initCleanup = options?.init?.(consumer);
  return consumer;
}

// 3. Standalone Operations
export function pushToConsumer<VALUE>(state: ConsumerState<VALUE>, value: VALUE): void {
  // Prevent execution if already terminated
  if (state.status === "abort" || state.status === "complete") return;

  if (state.credit > 0 && !state.queue?.size) {
    state.handler(state, value);
    state.credit--;
  } else {
    state.queue ??= state.options?.queueFactory?.() ?? new DefaultQueue();
    state.queue.enqueue(value);
    state.options?.enqueue?.(state, value);
  }
  state.options?.push?.(state, value);
}

export function advanceConsumer<VALUE>(state: ConsumerState<VALUE>): void {
  if (state.status === "abort" || state.status === "complete") return;

  state.credit++;

  if (!state.queue?.size) {
    state.options?.next?.(state);
    return;
  }

  if (state.credit > 1) return;

  while (state.credit > 0) {
    const value = state.queue.dequeue();

    if (value === EMPTY) {
      state.queue = undefined;
      if (state.status === "drain") {
        terminateConsumer(state, "complete");
      } else {
        state.options?.next?.(state);
      }
      break;
    }
    state.options?.dequeue?.(state, value);
    state.handler(state, value);
    state.credit--;
  }
}

export function terminateConsumer<VALUE>(state: ConsumerState<VALUE>, reason: TerminateReason): void {
  if (state.status === "abort" || state.status === "complete") return;

  if (reason === "abort") {
    state.status = "abort";
    state.queue?.clear();
  } else if (state.queue?.size) {
    state.status = "drain";
    state.options?.drain?.(state);
    return;
  } else {
    state.status = "complete";
  }

  state.initCleanup?.(reason);
  state.options?.terminate?.(state, reason);

  // Clean up references for GC (Garbage Collection)
  state.queue = state.options = undefined;
  state.handler = EMPTY_FUNCTION;
}

export namespace Consumer {
  export type Status = "active" | "drain" | TerminateReason;
  export type Handler<VALUE> = (consumer: Consumer<VALUE>, value: VALUE) => void;
  export type InitCleanup = (reason: TerminateReason) => void;
  export type Options<VALUE> = {
    passive?: boolean;
    queueFactory?: () => Queue<VALUE>;
    init?: (consumer: Consumer<VALUE>) => undefined | InitCleanup;
    push?: (consumer: Consumer<VALUE>, value: VALUE) => void;
    next?: (consumer: Consumer<VALUE>) => void;
    drain?: (consumer: Consumer<VALUE>) => void;
    terminate?: (consumer: Consumer<VALUE>, reason: TerminateReason) => void;
    enqueue?: (consumer: Consumer<VALUE>, value: VALUE) => void;
    dequeue?: (consumer: Consumer<VALUE>, value: VALUE) => void;
  };
}
