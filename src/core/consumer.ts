import type { TerminateReason } from "./types";
import { EMPTY } from "./consts";
import { DefaultQueue } from "./default-queue";
import { Queue } from "./queue";

export abstract class Consumer<T> {
  private status: Consumer.Status = "active";
  private queue?: Queue<T>;
  private credit = 0;
  abstract handler(consumer: Consumer<T>, value: T): void;
  protected push(consumer: Consumer<T>, value: T) {}
  protected next(consumer: Consumer<T>) {}
  protected drain(consumer: Consumer<T>) {}
  protected enqueue(consumer: Consumer<T>, value: T) {}
  protected dequeue(consumer: Consumer<T>, value: T) {}
  protected terminate(consumer: Consumer<T>, reason: TerminateReason) {}
  static push<T>(consumer: Consumer<T>, value: T) {
    if (consumer.credit > 0 && !consumer.queue?.size) {
      consumer.handler(consumer, value);
      consumer.credit--;
    } else {
      (consumer.queue ??= new DefaultQueue()).enqueue(value);
      consumer.enqueue(consumer, value);
    }
    consumer.push(consumer, value);
  }
  static next<T>(consumer: Consumer<T>) {
    consumer.credit++;

    if (!consumer.queue?.size) {
      consumer.next(consumer);
      return this;
    }

    if (consumer.credit > 1) return this;

    while (consumer.credit > 0) {
      const value = consumer.queue.dequeue();

      if (value === EMPTY) {
        consumer.queue = undefined;
        if (consumer.status === "drain") {
          consumer.terminate(consumer, "complete");
        } else {
          consumer.next(consumer);
        }
        break;
      }
      consumer.dequeue(consumer, value);

      consumer.handler(consumer, value);
      consumer.credit--;
    }
    return this;
  }
  static terminate<T>(consumer: Consumer<T>, reason: TerminateReason) {
    if (reason === "abort") {
      consumer.status = "abort";
      consumer.queue?.clear();
    } else if (consumer.queue?.size) {
      consumer.status = "drain";
      consumer.drain(consumer);
      return this;
    } else {
      consumer.status = "complete";
    }

    consumer.terminate(consumer, reason);

    consumer.queue = undefined;
  }
}

export namespace Consumer {
  export type AnyConsumer = Consumer<any>;
  export type Status = "active" | "drain" | TerminateReason;
}
