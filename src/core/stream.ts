import { Consumer } from "./consumer";
import { NonEmptyString, Queue, Consumable, TerminateReason, Terminable, ConsumerSet, AnySource } from "./types";

import { EMPTY_THIS_FUNCTION } from "./consts";
import { SetConsumerSet } from "./set-consumer-set";
import { Source } from "./source";
import { LinkedListQueue } from "./linked-list-queue";

export class Stream<VALUE>
  extends Source<VALUE>
  implements Consumable<VALUE>, Terminable, Disposable, AsyncIterable<VALUE>
{
  private _consumerSet?: ConsumerSet<VALUE>;
  private _status: Stream.Status;
  private _pulling: boolean;
  private _initCleanup?: (reason: TerminateReason) => void;
  private _sourceConsumer?: Consumer<VALUE>;
  private _signalConsumer?: Consumer<TerminateReason>;

  private _source?: Consumable<VALUE>;
  private _consumerSetFactory?: () => ConsumerSet<VALUE> = undefined;
  private _consumerQueueFactory?: () => Queue<VALUE>;
  private _push?: (stream: Stream<VALUE>, value: VALUE) => void;
  private _next?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
  private _consumerJoin?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
  private _consumerLeft?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
  private _firstConsumerJoin?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
  private _lastConsumerLeft?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
  private _drain?: (stream: Stream<VALUE>) => void;
  private _terminate?: (stream: Stream<VALUE>, reason: TerminateReason) => void;

  private _$push?: Stream<VALUE>;
  private _$next?: Stream<Consumer<VALUE>>;
  private _$drain?: Stream<void>;
  private _$consumerJoin?: Stream<Consumer<VALUE>>;
  private _$consumerLeft?: Stream<Consumer<VALUE>>;
  private _$firstConsumerJoin?: Stream<Consumer<VALUE>>;
  private _$lastConsumerLeft?: Stream<Consumer<VALUE>>;
  private _$terminate?: Stream<TerminateReason>;

  constructor(options?: Stream.Options<VALUE>) {
    super();

    this._source = options?.source;

    this._consumerSetFactory = options?.consumerSetFactory;
    this._consumerQueueFactory = options?.consumerQueueFactory;
    this._push = options?.push;
    this._next = options?.next;
    this._consumerJoin = options?.consumerJoin;
    this._consumerLeft = options?.consumerLeft;
    this._firstConsumerJoin = options?.firstConsumerJoin;
    this._lastConsumerLeft = options?.lastConsumerLeft;
    this._drain = options?.drain;
    this._terminate = options?.terminate;

    this._status = "active";
    this._pulling = false;

    this._signalConsumer = options?.signal?.consume((_, reason) => this.terminate(reason)).next();

    if (this.status === "active") this._initCleanup = options?.init?.(this);
  }
  async *[Symbol.asyncIterator]() {
    const buffer = new LinkedListQueue<VALUE>();
    let resolve: (() => void) | undefined;

    const consumer = this.consume((_, value) => {
      buffer.enqueue(value);
      resolve?.();
    });

    try {
      while (consumer.status === "active" || consumer.status === "drain" || buffer.size > 0) {
        if (buffer.size === 0) {
          await new Promise<void>((r) => (resolve = r));
          resolve = undefined;
        }
        while (buffer.size > 0) {
          yield buffer.dequeue() as VALUE;
        }
        consumer.next();
      }
    } catch (e) {
      consumer.terminate("abort");
    } finally {
      consumer.terminate("complete");
    }
  }

  [Symbol.dispose]() {
    this.terminate("abort");
  }

  get consumersCount(): number {
    return this._consumerSet?.size ?? 0;
  }
  get status(): Stream.Status {
    return this._status;
  }
  get $push(): Consumable<VALUE> {
    return (this._$push ??= new Stream<VALUE>({
      lastConsumerLeft: () => (this._$push = undefined),
    })).asSource();
  }
  get $next(): Consumable<Consumer<VALUE>> {
    return (this._$next ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._$next = undefined),
    })).asSource();
  }
  get $consumerJoin(): Consumable<Consumer<VALUE>> {
    return (this._$consumerJoin ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._$consumerJoin = undefined),
    })).asSource();
  }
  get $consumerLeft(): Consumable<Consumer<VALUE>> {
    return (this._$consumerLeft ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._$consumerLeft = undefined),
    })).asSource();
  }
  get $firstConsumerJoin(): Consumable<Consumer<VALUE>> {
    return (this._$firstConsumerJoin ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._$firstConsumerJoin = undefined),
    })).asSource();
  }
  get $lastConsumerLeft(): Consumable<Consumer<VALUE>> {
    return (this._$lastConsumerLeft ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._$lastConsumerLeft = undefined),
    })).asSource();
  }
  get $drain(): Consumable<void> {
    return (this._$drain ??= new Stream<void>({
      lastConsumerLeft: () => (this._$drain = undefined),
    })).asSource();
  }
  get $terminate(): Consumable<TerminateReason> {
    return (this._$terminate ??= new Stream<TerminateReason>({
      lastConsumerLeft: () => (this._$terminate = undefined),
      consumerJoin: (stream, consumer) => {
        if (this._status === "abort" || this._status === "complete") {
          consumer.push(this._status);
          consumer.terminate(this._status);
          stream.terminate(this._status);
        }
      },
    })).asSource();
  }

  push(value: VALUE): this {
    this._pulling = false;
    this._consumerSet?.push(value);
    this._push?.(this, value);
    this._$push?.push(value);
    return this;
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    options = { ...options };

    const consumer = new Consumer(handler, {
      ...options,
      queueFactory: options.queueFactory ?? this._consumerQueueFactory,
      next: (consumer) => {
        if (this._pulling === false) {
          this._pulling = true;
          this._sourceConsumer?.next();
          this._next?.(this, consumer);
          this._$next?.push(consumer);
        }
        options.next?.(consumer);
      },

      terminate: (consumer, reason) => {
        if (deleteConsumer()) {
          this._consumerLeft?.(this, consumer);
          this._$consumerLeft?.push(consumer);

          if (!this._consumerSet?.size) {
            this._consumerSet = undefined;

            this._lastConsumerLeft?.(this, consumer);
            this._$lastConsumerLeft?.push(consumer);

            if (this._status === "drain") this.terminate("complete");
          }
        }

        options.terminate?.(consumer, reason);
      },
    });

    if (!this._consumerSet) this._consumerSet = this._consumerSetFactory?.() ?? new SetConsumerSet();

    const deleteConsumer = this._consumerSet.add(consumer);

    if (this._consumerSet.size === 1) {
      this._firstConsumerJoin?.(this, consumer);
      this._$firstConsumerJoin?.push(consumer);

      if (this._source)
        this._sourceConsumer = this._source?.consume(
          (_, value) => {
            this._pulling = false;
            this._consumerSet?.push(value);
            this._push?.(this, value);
            this._$push?.push(value);
          },
          {
            terminate: (_, reason) => {
              this.terminate(reason);
            },
          },
        );
    }

    this._consumerJoin?.(this, consumer);
    this._$consumerJoin?.push(consumer);

    return consumer;
  }
  terminate(reason: TerminateReason): this {
    this.push = EMPTY_THIS_FUNCTION;
    this.consume = (handler, options) => new Consumer(handler, options).terminate(reason);

    if (reason === "abort") {
      this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "abort";
    } else if (this.consumersCount) {
      this._status = "drain";
      this._drain?.(this);
      this._$drain?.push();

      this._consumerSet?.terminate("complete");
      this._consumerSet = undefined;

      return this;
    } else {
      this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "complete";
    }

    this._initCleanup?.(reason);
    this._consumerSet?.terminate(reason);
    this._sourceConsumer?.terminate(reason);
    this._signalConsumer?.terminate(reason);
    this._$push?.terminate(reason);
    this._$next?.terminate(reason);
    this._$drain?.terminate(reason);
    this._$consumerJoin?.terminate(reason);
    this._$consumerLeft?.terminate(reason);
    this._$firstConsumerJoin?.terminate(reason);
    this._$lastConsumerLeft?.terminate(reason);

    this._terminate?.(this, reason);
    this._$terminate?.push(reason);
    this._$terminate?.terminate(reason);

    this._source =
      this._consumerQueueFactory =
      this._push =
      this._next =
      this._consumerJoin =
      this._consumerLeft =
      this._firstConsumerJoin =
      this._lastConsumerLeft =
      this._drain =
      this._terminate =
      this._consumerSet =
      this._sourceConsumer =
      this._signalConsumer =
      this._$push =
      this._$next =
      this._$drain =
      this._$consumerJoin =
      this._$consumerLeft =
      this._$terminate =
        undefined;
    return this;
  }

  asSource(): Consumable<VALUE> {
    return {
      consume: this.consume.bind(this),
    };
  }
}

export namespace Stream {
  export type Status = "active" | "drain" | TerminateReason;

  export type Options<VALUE> = {
    source?: Consumable<VALUE>;
    signal?: Consumable<TerminateReason>;
    consumerSetFactory?: () => ConsumerSet<VALUE>;
    consumerQueueFactory?: () => Queue<VALUE>;
    init?: (stream: Stream<VALUE>) => undefined | ((reason: TerminateReason) => void);
    push?: (stream: Stream<VALUE>, value: VALUE) => void;
    next?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    consumerJoin?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    consumerLeft?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    firstConsumerJoin?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    lastConsumerLeft?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    drain?: (stream: Stream<VALUE>) => void;
    terminate?: (stream: Stream<VALUE>, reason: TerminateReason) => void;
  };
}
