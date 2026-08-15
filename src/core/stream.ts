import { Queue, Consumable, TerminateReason, Terminable, ConsumerSet } from "./types";
import { Consumer } from "./consumer";
import { EMPTY_THIS_FUNCTION } from "./consts";
import { DefaultConsumerSet } from "./default-consumer-set";
import { Source } from "./source";
import { SourceProxy } from "./source-proxy";

export class Stream<VALUE> extends Source<VALUE> implements Terminable, Disposable, AsyncIterable<VALUE> {
  private _options: Stream.Options<VALUE>;
  private _events: Stream.Events<VALUE>;
  private _consumerSet: ConsumerSet<VALUE>;
  private _status: Stream.Status;
  private _pulling: boolean;
  private _sourceProxy?: Source<VALUE>;
  private _initCleanup?: (reason: TerminateReason) => void;
  private _sourceConsumer?: Consumer<VALUE>;
  private _signalConsumer?: Consumer<TerminateReason>;

  constructor(options?: Stream.Options<VALUE>) {
    super();
    this._options = { ...options };
    this._events = {};

    this._consumerSet = this._options.consumerSetFactory?.() ?? new DefaultConsumerSet();
    this._status = "active";
    this._pulling = false;

    this._signalConsumer = this._options.$signal?.consume((_, reason) => this.terminate(reason)).next();

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
  get $push(): Consumable<VALUE> {
    return (this._events.$push ??= new Stream<VALUE>({
      lastConsumerLeft: () => (this._events.$push = undefined),
    })).asSource();
  }
  get $next(): Consumable<Consumer<VALUE>> {
    return (this._events.$next ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._events.$next = undefined),
    })).asSource();
  }
  get $consumerJoin(): Consumable<Consumer<VALUE>> {
    return (this._events.$consumerJoin ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._events.$consumerJoin = undefined),
    })).asSource();
  }
  get $consumerLeft(): Consumable<Consumer<VALUE>> {
    return (this._events.$consumerLeft ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._events.$consumerLeft = undefined),
    })).asSource();
  }
  get $firstConsumerJoin(): Consumable<Consumer<VALUE>> {
    return (this._events.$firstConsumerJoin ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._events.$firstConsumerJoin = undefined),
    })).asSource();
  }
  get $lastConsumerLeft(): Consumable<Consumer<VALUE>> {
    return (this._events.$lastConsumerLeft ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._events.$lastConsumerLeft = undefined),
    })).asSource();
  }
  get $drain(): Consumable<void> {
    return (this._events.$drain ??= new Stream<void>({
      lastConsumerLeft: () => (this._events.$drain = undefined),
    })).asSource();
  }
  get $terminate(): Consumable<TerminateReason> {
    return (this._events.$terminate ??= new Stream<TerminateReason>({
      lastConsumerLeft: () => (this._events.$terminate = undefined),
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
    this._consumerSet.push(value);
    this._options.push?.(this, value);
    this._events.$push?.push(value);
    return this;
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { queueFactory, next, terminate, ...rest } = options ?? {};
    const consumer = new Consumer(handler, {
      ...rest,
      queueFactory: queueFactory ?? this._options?.consumerQueueFactory,
      next: (consumer) => {
        if (this._pulling === false) {
          this._pulling = true;
          this._sourceConsumer?.next();
          this._options.next?.(this, consumer);
          this._events.$next?.push(consumer);
        }
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
    this._sourceConsumer?.terminate(reason);
    this._signalConsumer?.terminate(reason);

    this._options.terminate?.(this, reason);
    this._events.$terminate?.push(reason);

    Object.values(this._events).forEach((stream) => stream.terminate(reason));

    this._options = this._events = {};
    this._sourceConsumer = this._signalConsumer = undefined;
    return this;
  }
  asSource(): Consumable<VALUE> {
    return (this._sourceProxy ??= new SourceProxy(this));
  }
  private _addConsumer(consumer: Consumer<VALUE>): ConsumerSet.Delete {
    const deleteConsumer = this._consumerSet.add(consumer);

    if (this._consumerSet.size === 1) {
      this._options.firstConsumerJoin?.(this, consumer);
      this._events.$firstConsumerJoin?.push(consumer);
      if (this._options.$source)
        this._sourceConsumer = this._options.$source.consume((_, value) => this.push(value), {
          terminate: (_, reason) => {
            this.terminate(reason);
          },
        });
    }

    this._options.consumerJoin?.(this, consumer);
    this._events.$consumerJoin?.push(consumer);
    return deleteConsumer;
  }
}

export namespace Stream {
  export type Status = "active" | "drain" | TerminateReason;

  export type Options<VALUE> = {
    $source?: Consumable<VALUE>;
    $signal?: Consumable<TerminateReason>;
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
