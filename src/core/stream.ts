import { TerminateReason } from "./types";
import { Consumer } from "./consumer";
import { EMPTY_THIS_FUNCTION } from "./consts";
import { DefaultConsumerSet } from "./default-consumer-set";
import { Source } from "./source";
import { ConsumerSet } from "./consumer-set";
import { Consumable } from "./consumable";

/**
 * A multicast push source that broadcasts values to all its {@link Consumer}s.
 *
 * `Stream` is the primary building block for hot event sources. It extends {@link Source}
 * and exposes lifecycle event streams (`$push`, `$next`, `$consumerJoin`, etc.) as
 * first-class `Source`s so you can observe internal stream activity without special tooling.
 *
 * When consuming `$next` source or providing `next` option hook these ones are triggered by the fastest {@link Consumer},
 * so all {@link Consumer}s will get the pushed value and may buffer it if their queues are not empty or they are asynchronous  ,
 * that's mean fastest {@link Consumer} is the dirver and the others are the followers but this can change if the fastest become slower,
 * and we can determine/force the driver by making the other {@link Consumer}s a passive ones by piping `passive` transformer
 *
 * @template VALUE The type of values emitted.
 *@param options Stream options including lifecycle hooks.
 * @example
 * const stream = new Stream<number>();
 * stream.consume((consumer, value) => { console.log(value); consumer.next(); }).next();
 * stream.pushMany(1,2,3);
 * stream.terminate("complete");
 */
export class Stream<VALUE> extends Source<VALUE> implements Disposable, AsyncDisposable, AsyncIterable<VALUE> {
  private _options: Stream.Options<VALUE>;
  private _events: Stream.Events<VALUE>;
  private _consumerSet: ConsumerSet<VALUE>;
  private _status: Stream.Status;
  private _initCleanup?: (reason: TerminateReason) => void;
  private pulling = false;

  constructor(options?: Stream.Options<VALUE>) {
    super();
    this._options = options ?? {};
    this._events = {};

    this._consumerSet = this._options.consumerSetFactory?.() ?? new DefaultConsumerSet();
    this._status = "active";

    if (this.status === "active") this._initCleanup = this._options?.init?.(this);
  }
  /** Terminates the stream with `"complete"` when used with the `await using` keyword. */
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
  /** Terminates the stream with `"abort"` when used with the `using` keyword. */
  [Symbol.dispose]() {
    this.terminate("abort");
  }
  get options(): Stream.Options<VALUE> {
    return this._options;
  }
  set options(options: Stream.Options<VALUE>) {
    this._options = options;
  }
  getOption<NAME extends keyof Stream.Options<VALUE>>(name: NAME): Stream.Options<VALUE>[NAME] {
    return this._options[name];
  }
  setOption<NAME extends keyof Stream.Options<VALUE>>(name: NAME, value: Stream.Options<VALUE>[NAME]) {
    this._options[name] = value;
  }
  get consumersCount(): number {
    return this._consumerSet?.size ?? 0;
  }
  get status(): Stream.Status {
    return this._status;
  }
  /** Emits every value passed to {@link push}. */
  get $push(): Source<VALUE> {
    return Source.from(
      (this._events.$push ??= new Stream<VALUE>({
        lastConsumerLeft: () => (this._events.$push = undefined),
      })),
    );
  }
  /** Emits the fastest {@link Consumer} that called `next()`. */
  get $next(): Source<Consumer<VALUE>> {
    return Source.from(
      (this._events.$next ??= new Stream<Consumer<VALUE>>({
        lastConsumerLeft: () => (this._events.$next = undefined),
      })),
    );
  }
  /** Emits every {@link Consumer} created via {@link consume}. */
  get $consumerJoin(): Source<Consumer<VALUE>> {
    return Source.from(
      (this._events.$consumerJoin ??= new Stream<Consumer<VALUE>>({
        lastConsumerLeft: () => (this._events.$consumerJoin = undefined),
      })),
    );
  }
  /** Emits every {@link Consumer} terminated independently or terminated by the stream termination. */
  get $consumerLeft(): Source<Consumer<VALUE>> {
    return Source.from(
      (this._events.$consumerLeft ??= new Stream<Consumer<VALUE>>({
        lastConsumerLeft: () => (this._events.$consumerLeft = undefined),
      })),
    );
  }
  /** Emits the first {@link Consumer} created when the stream had no consumers. */
  get $firstConsumerJoin(): Source<Consumer<VALUE>> {
    return Source.from(
      (this._events.$firstConsumerJoin ??= new Stream<Consumer<VALUE>>({
        lastConsumerLeft: () => (this._events.$firstConsumerJoin = undefined),
      })),
    );
  }
  /** Emits the last {@link Consumer} to leave when the stream becomes empty. */
  get $lastConsumerLeft(): Source<Consumer<VALUE>> {
    return Source.from(
      (this._events.$lastConsumerLeft ??= new Stream<Consumer<VALUE>>({
        lastConsumerLeft: () => (this._events.$lastConsumerLeft = undefined),
      })),
    );
  }
  /** Emits `void` when the stream enters the drain state. */
  get $drain(): Source<void> {
    return Source.from(
      (this._events.$drain ??= new Stream<void>({
        lastConsumerLeft: () => (this._events.$drain = undefined),
      })),
    );
  }
  /**
   * Emits the {@link TerminateReason} when the stream terminates.
   * Late {@link Consumer}s created receive the reason immediately if already terminated.
   */
  get $terminate(): Source<TerminateReason> {
    return Source.from(
      (this._events.$terminate ??= new Stream<TerminateReason>({
        lastConsumerLeft: () => (this._events.$terminate = undefined),
        consumerJoin: (s, c) => {
          if (this._status === "abort" || this._status === "complete") {
            c.push(this._status).next();
            c.terminate(this._status);
            s.terminate(this._status);
          }
        },
      })),
    );
  }
  /**
   * Broadcasts `value` to all consumers.
   * Replaced with a no-op after termination.
   */
  push(value: VALUE): this {
    this.pulling = false;
    this._options.push?.(this, value);
    this._events.$push?.push(value);
    this._consumerSet.push(value);
    return this;
  }
  /**This is for inline values to push */
  pushMany(...values: [VALUE, ...VALUE[]]): this {
    return this.pushBatch(values);
  }
  /**This is for pushing a big set of values */
  pushBatch(values: VALUE[]): this {
    for (let i = 0; i < values.length; i++) {
      this.push(values[i]);
    }
    return this;
  }
  /**
   * create a new consumer for this stream.
   * Fires `firstConsumerJoin` and `consumerJoin` lifecycle hooks.
   *
   * @param handler The value handler.
   * @param options Consumer options including lifecycle hooks.
   * @returns  - {@link Consumer}
   */
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { queueFactory, next, terminate, ...rest } = options ?? {};

    const consumer = new Consumer(handler, {
      ...rest,
      next: (consumer) => {
        if (!this.pulling) {
          this.pulling = true;
          this._options.next?.(this, consumer);
          this._events.$next?.push(consumer);
          next?.(consumer);
        }
      },

      terminate: (consumer, reason) => {
        if (this._consumerSet.delete(consumer)) {
          this._options.consumerLeft?.(this, consumer);
          this._events.$consumerLeft?.push(consumer);

          if (!this._consumerSet.size) {
            this._options.lastConsumerLeft?.(this, consumer);
            this._events.$lastConsumerLeft?.push(consumer);
            if (this._status === "drain") this.terminate("complete");
          }
        }

        terminate?.(consumer, reason);
      },
    });

    this._consumerSet.add(consumer);

    if (this._consumerSet.size === 1) {
      this._options.firstConsumerJoin?.(this, consumer);
      this._events.$firstConsumerJoin?.push(consumer);
    }

    this._options.consumerJoin?.(this, consumer);
    this._events.$consumerJoin?.push(consumer);

    return consumer;
  }
  /**
   * Terminates the stream.
   *
   * - `"abort"`: immediately stops, clears all consumer queues.
   * - `"complete"`: drains remaining consumer queues before stopping.
   *
   * @param reason The termination reason.
   */
  terminate(reason: TerminateReason): this {
    this.push = EMPTY_THIS_FUNCTION;
    this.consume = (handler, options) => new Consumer(handler, options).terminate(reason);

    if (reason === "abort") {
      this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "abort";
    } else if (this.consumersCount) {
      this._status = "drain";
      this._options.drain?.(this);
      this._events.$drain?.push();

      this._consumerSet.terminate("complete");

      return this;
    } else {
      this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "complete";
    }

    this._consumerSet.terminate(reason);
    this._initCleanup?.(reason);

    this._options.terminate?.(this, reason);
    this._events.$terminate?.push(reason);

    Object.values(this._events).forEach((stream) => stream.terminate(reason));

    this._options = this._events = {};

    return this;
  }

  /**
   * Creates a `Stream` that pulls from a {@link Consumable} source.
   * Bridges unicast sources into a multicast `Stream`, pulling from the source
   * is triggered by the faster consumer and all consumer will get the value,
   * so slower ones will buffer the value and consume it at their own pace.
   *
   * @param consumable The upstream consumable to pull from.
   * @param options Optional stream options.
   */
  static override from<VALUE>(consumable: Consumable<VALUE>, options?: Stream.Options<VALUE>): Stream<VALUE> {
    const { firstConsumerJoin, next, terminate, ...rest } = options ?? {};

    let consumableConsumer: Consumer<VALUE>;

    return new Stream({
      ...rest,
      firstConsumerJoin(stream, consumer) {
        consumableConsumer = consumable.consume((_, value) => stream.push(value), {
          terminate: (_, reason) => stream.terminate(reason),
        });
        firstConsumerJoin?.(stream, consumer);
      },

      next(stream, consumer) {
        consumableConsumer.next();
        next?.(stream, consumer);
      },
      terminate(stream, reason) {
        consumableConsumer.terminate(reason);
        terminate?.(stream, reason);
      },
    });
  }
}

export namespace Stream {
  export type AnyStream = Stream<any>;
  export type Status = "active" | "drain" | TerminateReason;

  export interface Options<VALUE> {
    consumerSetFactory?: () => ConsumerSet<VALUE>;
    init?: (stream: Stream<VALUE>) => undefined | ((reason: TerminateReason) => void);
    push?: (stream: Stream<VALUE>, value: VALUE) => void;
    next?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    consumerJoin?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    consumerLeft?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    firstConsumerJoin?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    lastConsumerLeft?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    drain?: (stream: Stream<VALUE>) => void;
    terminate?: (stream: Stream<VALUE>, reason: TerminateReason) => void;
  }

  export type Events<VALUE> = {
    $push?: Stream<VALUE>;
    $next?: Stream<Consumer<VALUE>>;
    $drain?: Stream<void>;
    $consumerJoin?: Stream<Consumer<VALUE>>;
    $consumerLeft?: Stream<Consumer<VALUE>>;
    $firstConsumerJoin?: Stream<Consumer<VALUE>>;
    $lastConsumerLeft?: Stream<Consumer<VALUE>>;
    $terminate?: Stream<TerminateReason>;
  };
}
