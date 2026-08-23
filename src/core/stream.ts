import { TerminateReason, Terminable } from "./types";
import { Consumer } from "./consumer";
import { EMPTY_THIS_FUNCTION } from "./consts";
import { DefaultConsumerSet } from "./default-consumer-set";
import { Source } from "./source";
import { ConsumerSet } from "./consumer-set";
import { Consumable } from "./consumable";
import { Queue } from "./queue";

export class Stream<VALUE> extends Source<VALUE> implements Terminable, Disposable, AsyncIterable<VALUE> {
  private _options: Stream.Options<VALUE>;
  private _events: Stream.Events<VALUE>;
  private _consumerSet: ConsumerSet<VALUE>;
  private _status: Stream.Status;
  private _initCleanup?: (reason: TerminateReason) => void;

  constructor(options?: Stream.Options<VALUE>) {
    super();
    this._options = { ...options };
    this._events = {};

    this._consumerSet = this._options.consumerSetFactory?.() ?? new DefaultConsumerSet();
    this._status = "active";

    if (this.status === "active") this._initCleanup = this._options?.init?.(this);
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
  get $push(): Source<VALUE> {
    return Source.from(
      (this._events.$push ??= new Stream<VALUE>({
        lastConsumerLeft: () => (this._events.$push = undefined),
      })),
    );
  }
  get $next(): Source<Consumer<VALUE>> {
    return Source.from(
      (this._events.$next ??= new Stream<Consumer<VALUE>>({
        lastConsumerLeft: () => (this._events.$next = undefined),
      })),
    );
  }
  get $consumerJoin(): Source<Consumer<VALUE>> {
    return Source.from(
      (this._events.$consumerJoin ??= new Stream<Consumer<VALUE>>({
        lastConsumerLeft: () => (this._events.$consumerJoin = undefined),
      })),
    );
  }
  get $consumerLeft(): Source<Consumer<VALUE>> {
    return Source.from(
      (this._events.$consumerLeft ??= new Stream<Consumer<VALUE>>({
        lastConsumerLeft: () => (this._events.$consumerLeft = undefined),
      })),
    );
  }
  get $firstConsumerJoin(): Source<Consumer<VALUE>> {
    return Source.from(
      (this._events.$firstConsumerJoin ??= new Stream<Consumer<VALUE>>({
        lastConsumerLeft: () => (this._events.$firstConsumerJoin = undefined),
      })),
    );
  }
  get $lastConsumerLeft(): Source<Consumer<VALUE>> {
    return Source.from(
      (this._events.$lastConsumerLeft ??= new Stream<Consumer<VALUE>>({
        lastConsumerLeft: () => (this._events.$lastConsumerLeft = undefined),
      })),
    );
  }
  get $drain(): Source<void> {
    return Source.from(
      (this._events.$drain ??= new Stream<void>({
        lastConsumerLeft: () => (this._events.$drain = undefined),
      })),
    );
  }
  get $terminate(): Source<TerminateReason> {
    return Source.from(
      (this._events.$terminate ??= new Stream<TerminateReason>({
        lastConsumerLeft: () => (this._events.$terminate = undefined),
        consumerJoin: (stream, consumer) => {
          if (this._status === "abort" || this._status === "complete") {
            consumer.push(this._status);
            consumer.terminate(this._status);
            stream.terminate(this._status);
          }
        },
      })),
    );
  }
  push(value: VALUE): this {
    this._options.push?.(this, value);
    this._events.$push?.push(value);
    this._consumerSet.push(value);
    return this;
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { queueFactory, next, terminate, ...rest } = options ?? {};

    const consumer = new Consumer(handler, {
      ...rest,
      queueFactory: queueFactory ?? this._options?.consumerQueueFactory,
      next: (consumer) => {
        this._options.next?.(this, consumer);
        this._events.$next?.push(consumer);
        next?.(consumer);
      },

      terminate: (consumer, reason) => {
        if (deleteConsumer()) {
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

    const deleteConsumer = this._addConsumer(consumer);

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
  private _addConsumer(consumer: Consumer<VALUE>): ConsumerSet.Delete {
    const deleteConsumer = this._consumerSet.add(consumer);

    if (this._consumerSet.size === 1) {
      this._options.firstConsumerJoin?.(this, consumer);
      this._events.$firstConsumerJoin?.push(consumer);
    }

    this._options.consumerJoin?.(this, consumer);
    this._events.$consumerJoin?.push(consumer);
    return deleteConsumer;
  }
  static override from<VALUE>(consumable: Consumable<VALUE>, options?: Stream.Options<VALUE>): Stream<VALUE> {
    const { firstConsumerJoin, push, next, terminate, ...rest } = options ?? {};

    let pulling = false;
    let consumableConsumer: Consumer<VALUE>;

    return new Stream({
      ...rest,
      firstConsumerJoin(stream, consumer) {
        consumableConsumer = consumable.consume((_, value) => stream.push(value), {
          terminate: (_, reason) => stream.terminate(reason),
        });
        firstConsumerJoin?.(stream, consumer);
      },
      push(stream, value) {
        pulling = false;
        push?.(stream, value);
      },
      next(stream, consumer) {
        if (!pulling) {
          pulling = true;
          consumableConsumer.next();
        }
        next?.(stream, consumer);
      },
      terminate(stream, reason) {
        terminate?.(stream, reason);
      },
    });
  }
}

export namespace Stream {
  export type AnyStream = Stream<any>;
  export type Status = "active" | "drain" | TerminateReason;

  export type Options<VALUE> = {
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
