import type { TerminateReason } from "./types";
import { EMPTY, EMPTY_FUNCTION, EMPTY_THIS_FUNCTION } from "./consts";
import { DefaultQueue } from "./default-queue";
import { Queue } from "./queue";

/**
 * The fundamental unit of consumption in the stream engine.
 *
 * A `Consumer` receives values one at a time and controls its own throughput
 * via an explicit **credit system**. It only processes a value when it has
 * available credit (granted by calling {@link next}).
 *
 * Values pushed without available credit are queued and drained in order
 * as credit is granted.
 *
 * @template VALUE The type of values this consumer handles.
 *
 * @example
 * const consumer = new Consumer<number>((self, value) => {
 *   console.log(value);
 *   self.next(); // grant credit for the next value
 * });
 * consumer.next(); // grant initial credit
 * consumer.push(1);
 * consumer.push(2);
 */
export class Consumer<VALUE> implements Disposable, AsyncDisposable {
  /** The current handler function. Replaced with a no-op after termination. */
  private _handler: Consumer.Handler<VALUE>;
  /** Consumer options including queue factory, lifecycle hooks, and credit callbacks. */
  private _options: Consumer.Options<VALUE>;
  /** Current lifecycle status of this consumer. */
  private _status: Consumer.Status;
  /** Internal queue for values received without available credit. */
  private _queue?: Queue<VALUE>;
  /** Number of available credits (pending `next()` calls). */
  private _credit: number;

  /** Optional cleanup function returned by the `init` option. */
  private _initCleanup?: Consumer.InitCleanup;

  constructor(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>) {
    this._handler = handler;
    this._options = options ?? {};
    this._status = "active";
    this._queue = undefined;
    this._credit = 0;

    this._initCleanup = this._options?.init?.(this);
  }

  /** Terminates the consumer with `"complete"` when used with `await using` keyword. */
  async [Symbol.asyncDispose]() {
    await new Promise<void>((resolve) => {
      const terminate = this._options.terminate;
      this.setOption("terminate", (c, r) => {
        resolve();
        terminate?.(c, r);
      });
      this.terminate("complete");
    });
  }
  /** Terminates the consumer with `"abort"` when used with `using` keyword. */
  [Symbol.dispose]() {
    this.terminate("abort");
  }

  get handler(): Consumer.Handler<VALUE> {
    return this._handler;
  }
  set handler(handler: Consumer.Handler<VALUE>) {
    this._handler = handler;
  }
  get options(): Consumer.Options<VALUE> {
    return this._options;
  }
  set options(options: Consumer.Options<VALUE>) {
    this._options = options;
  }
  getOption<NAME extends keyof Consumer.Options<VALUE>>(name: NAME): Consumer.Options<VALUE>[NAME] {
    return this._options[name];
  }
  setOption<NAME extends keyof Consumer.Options<VALUE>>(name: NAME, value: Consumer.Options<VALUE>[NAME]) {
    this._options[name] = value;
  }
  get status(): Consumer.Status {
    return this._status;
  }

  get queue(): Queue<VALUE> | undefined {
    return this._queue;
  }

  get credit(): number {
    return this._credit;
  }

  /**
   * Delivers a value to this consumer.
   * If credit is available and the queue is empty, the handler is called immediately.
   * Otherwise the value is enqueued.
   *
   * @param value The value to deliver.
   */
  push(value: VALUE): this {
    if (this._credit > 0 && !this._queue?.size) {
      this._handler(this, value);

      this._credit--;
    } else {
      (this._queue ??= this._options.queueFactory?.() ?? new DefaultQueue()).enqueue(value);
    }

    return this;
  }
  pushMany(...values: [VALUE, ...VALUE[]]): this {
    return this.pushBatch(values);
  }
  pushBatch(values: VALUE[]): this {
    for (let i = 0; i < values.length; i++) {
      this.push(values[i]);
    }
    return this;
  }
  /**
   * Grants one credit, allowing the next queued value (or the next upstream pull)
   * to be processed. Must be called from within the handler to continue the stream.
   */
  next(): this {
    this._credit++;

    if (!this._queue?.size) {
      this._options.next?.(this);

      return this;
    }

    if (this._credit > 1) return this;

    while (this._credit > 0) {
      const value = this._queue.dequeue();

      if (value === EMPTY) {
        this._queue = undefined;
        if (this._status === "drain") {
          this.terminate("complete");
        } else {
          this._options.next?.(this);
        }
        break;
      }

      this._handler(this, value);
      this._credit--;
    }
    if (!this.queue?.size && this._status === "drain") {
      this._queue = undefined;
      this.terminate("complete");
    }
    return this;
  }
  /**
   * Terminates this consumer.
   *
   * - `"abort"`: immediately stops processing and clears the queue.
   * - `"complete"`: drains remaining queued values before stopping according to the consumption rate.
   *
   * @param reason The termination reason.
   */
  terminate(reason: TerminateReason): this {
    this.push = EMPTY_THIS_FUNCTION;
    if (reason === "abort") {
      this.next = this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "abort";
      this._queue?.clear();
    } else if (this._queue?.size) {
      this._status = "drain";
      this._options.drain?.(this);
      return this;
    } else {
      this.next = this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "complete";
    }

    this._initCleanup?.(reason);

    this._options.terminate?.(this, reason);

    this._queue = undefined;

    this._options = {};

    this._handler = EMPTY_FUNCTION;

    return this;
  }
}

export namespace Consumer {
  export type AnyConsumer = Consumer<any>;
  /** Possible lifecycle states of a consumer. */
  export type Status = "active" | "drain" | TerminateReason;
  /** The function called with each delivered value. */
  export type Handler<VALUE> = (consumer: Consumer<VALUE>, value: VALUE) => void;
  /** Cleanup function returned by the `init` option, called on termination. */
  export type InitCleanup = (reason: TerminateReason) => void;

  /**
   * Options for customising consumer behaviour.
   * All fields are optional lifecycle hooks.
   */
  export interface Options<VALUE> {
    queueFactory?: () => Queue<VALUE>;
    init?: (consumer: Consumer<VALUE>) => undefined | InitCleanup;
    next?: (consumer: Consumer<VALUE>) => void;
    drain?: (consumer: Consumer<VALUE>) => void;
    terminate?: (consumer: Consumer<VALUE>, reason: TerminateReason) => void;
  }
}
