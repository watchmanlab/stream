import { EMPTY, EMPTY_FUNCTION } from "./consts";
import { DefaultQueue } from "./default-queue";
import { Queue, TerminateReason } from "./types";

export interface Consumer<VALUE> {
  handler: Consumer.Handler<VALUE>;
  status: Consumer.Status;
  credit: number;
  queue?: Queue<VALUE>;
  initCleanup?: (reason: TerminateReason) => void;
  queueFactory?: () => Queue<VALUE>;
  init?: (consumer: Consumer<VALUE>) => undefined | Consumer.InitCleanup;
  push?: (consumer: Consumer<VALUE>, value: VALUE) => void;
  next?: (consumer: Consumer<VALUE>) => void;
  drain?: (consumer: Consumer<VALUE>) => void;
  terminate?: (consumer: Consumer<VALUE>, reason: TerminateReason) => void;
  enqueue?: (consumer: Consumer<VALUE>, value: VALUE) => void;
  dequeue?: (consumer: Consumer<VALUE>, value: VALUE) => void;
}

export namespace Consumer {
  export type Status = "active" | "drain" | TerminateReason;
  export type Handler<VALUE> = (consumer: Consumer<VALUE>, value: VALUE) => void;
  export type InitCleanup = (reason: TerminateReason) => void;
  export type Options<VALUE> = Omit<Consumer<VALUE>, "handler" | "status" | "credit" | "queue" | "initCleanup">;
  export function create<VALUE>(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const consumer: Consumer<VALUE> = {
      handler,
      status: "active",
      credit: 0,
      queue: undefined,
      queueFactory: options?.queueFactory,
      init: options?.init,
      push: options?.push,
      next: options?.next,
      drain: options?.drain,
      terminate: options?.terminate,
      enqueue: options?.enqueue,
      dequeue: options?.dequeue,
    };

    consumer.initCleanup = options?.init?.(consumer);

    return consumer;
  }
  export function push<VALUE>(consumer: Consumer<VALUE>, value: VALUE): void {
    if (consumer.status === "abort" || consumer.status === "complete") return;

    if (consumer.credit > 0 && !consumer.queue?.size) {
      consumer.handler(consumer, value);
      consumer.credit--;
    } else {
      consumer.queue ??= consumer.queueFactory?.() ?? new DefaultQueue();
      consumer.queue.enqueue(value);
      consumer.enqueue?.(consumer, value);
    }
    consumer.push?.(consumer, value);
  }
  export function next<VALUE>(consumer: Consumer<VALUE>): void {
    if (consumer.status === "abort" || consumer.status === "complete") return;

    consumer.credit++;

    if (!consumer.queue?.size) {
      consumer.next?.(consumer);
      return;
    }

    if (consumer.credit > 1) return;

    while (consumer.credit > 0) {
      const value = consumer.queue.dequeue();

      if (value === EMPTY) {
        consumer.queue = undefined;
        if (consumer.status === "drain") {
          terminate(consumer, "complete");
        } else {
          consumer.next?.(consumer);
        }
        break;
      }
      consumer.dequeue?.(consumer, value);
      consumer.handler(consumer, value);
      consumer.credit--;
    }
  }
  export function terminate<VALUE>(consumer: Consumer<VALUE>, reason: TerminateReason): void {
    if (consumer.status === "abort" || consumer.status === "complete") return;

    if (reason === "abort") {
      consumer.status = "abort";
      consumer.queue?.clear();
    } else if (consumer.queue?.size) {
      consumer.status = "drain";
      consumer.drain?.(consumer);
      return;
    } else {
      consumer.status = "complete";
    }

    consumer.initCleanup?.(reason);
    consumer.terminate?.(consumer, reason);

    // Clean up references for GC (Garbage Collection)
    consumer.queue = undefined;
    consumer.handler = EMPTY_FUNCTION;
  }
}
