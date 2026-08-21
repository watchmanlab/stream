import { TerminateReason } from "./types";
import { Consumer } from "./consumer";
import { EMPTY_THIS_FUNCTION } from "./consts";
import { DefaultConsumerSet } from "./default-consumer-set";
import { Source } from "./source";
import { Consumable } from "./consumable";
import { ConsumerSet } from "./consumer-set";

export class Stream<VALUE> extends Source<VALUE> implements Disposable, AsyncIterable<VALUE> {
  protected _options?: Stream.Options<VALUE>;
  protected _events: Stream.Events<VALUE>;
  protected _consumerSet: ConsumerSet<VALUE>;
  protected _status: Stream.Status;

  constructor(options?: Stream.Options<VALUE>) {
    super();
    this._options = options;
    this._events = {};
    this._consumerSet = options?.consumerSet ?? new DefaultConsumerSet();
    this._status = "active";
  }
  [Symbol.dispose]() {
    this.terminate("abort");
  }

  get status(): Stream.Status {
    return this._status;
  }
  get consumersCount(): number {
    return this._consumerSet.size;
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
    this._options?.push?.(this, value);
    this._events.$push?.push(value);
    this._consumerSet.push(value);
    return this;
  }
  consume(options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const consumer = new Consumer(new ConsumerOptions(this, options));

    this._consumerSet.add(consumer);

    if (this._consumerSet.size === 1) {
      this._options?.firstConsumerJoin?.(this, consumer);
      this._events.$firstConsumerJoin?.push(consumer);
    }

    this._options?.consumerJoin?.(this, consumer);
    this._events.$consumerJoin?.push(consumer);

    return consumer;
  }
  terminate(reason: TerminateReason): this {
    this.push = EMPTY_THIS_FUNCTION;
    this.consume = (options) => new Consumer(options).terminate(reason);

    if (reason === "abort") {
      this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "abort";
    } else if (this.consumersCount) {
      this._status = "drain";
      this._options?.drain?.(this);
      this._events.$drain?.push();

      this._consumerSet.terminate("complete");

      return this;
    } else {
      this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "complete";
    }

    this._consumerSet.terminate(reason);

    this._options?.terminate?.(this, reason);
    this._events.$terminate?.push(reason);

    Object.values(this._events).forEach((stream) => stream.terminate(reason));

    this._options = this._events = {};

    return this;
  }

  static override from<VALUE>(consumable: Consumable<VALUE>, options?: Stream.Options<VALUE>): Stream<VALUE> {
    const { firstConsumerJoin, push, next, terminate, ...rest } = options ?? {};

    const context = { pulling: false, consumableConsumer: undefined } satisfies StreamFromContext<VALUE>;

    return new Stream(new StreamFromOptions(consumable, context, options));
  }
}

export namespace Stream {
  export type AnyStream = Stream<any>;
  export type Status = "active" | "drain" | TerminateReason;

  export interface Options<VALUE> {
    consumerSet?: ConsumerSet<VALUE>;
    push?(stream: Stream<VALUE>, value: VALUE): void;
    next?(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void;
    consumerJoin?(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void;
    consumerLeft?(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void;
    firstConsumerJoin?(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void;
    lastConsumerLeft?(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void;
    drain?(stream: Stream<VALUE>): void;
    terminate?(stream: Stream<VALUE>, reason: TerminateReason): void;
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
  export class DefaultOptions<VALUE> implements Required<Options<VALUE>> {
    constructor(protected options?: Options<VALUE>) {}
    get consumerSet(): ConsumerSet<VALUE> {
      return new DefaultConsumerSet();
    }
    push(stream: Stream<VALUE>, value: VALUE): void {
      return this.options?.push?.(stream, value);
    }
    next(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void {
      return this.options?.next?.(stream, consumer);
    }
    drain(stream: Stream<VALUE>): void {
      return this.options?.drain?.(stream);
    }
    consumerJoin(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void {
      return this.options?.consumerJoin?.(stream, consumer);
    }
    consumerLeft(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void {
      return this.options?.consumerLeft?.(stream, consumer);
    }
    firstConsumerJoin(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void {
      return this.options?.firstConsumerJoin?.(stream, consumer);
    }
    lastConsumerLeft(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void {
      return this.options?.lastConsumerLeft?.(stream, consumer);
    }
    terminate(stream: Stream<VALUE>, reason: TerminateReason): void {
      return this.options?.terminate?.(stream, reason);
    }
  }
}
class ConsumerOptions<VALUE> extends Consumer.DefaultOptions<VALUE> {
  constructor(
    private stream: Stream<VALUE>,
    options?: Consumer.Options<VALUE>,
  ) {
    super(options);
  }
  override next(consumer: Consumer<VALUE>): void {
    this.stream["_options"]?.next?.(this.stream, consumer);
    this.stream["_events"].$next?.push(consumer);
    this.options?.next?.(consumer);
  }
  override terminate(consumer: Consumer<VALUE>, reason: TerminateReason): void {
    if (this.stream["_consumerSet"].delete(consumer)) {
      this.stream["_options"]?.consumerLeft?.(this.stream, consumer);
      this.stream["_events"].$consumerLeft?.push(consumer);

      if (!this.stream["_consumerSet"].size) {
        this.stream["_options"]?.lastConsumerLeft?.(this.stream, consumer);
        this.stream["_events"].$lastConsumerLeft?.push(consumer);
        if (this.stream["_status"] === "drain") this.stream.terminate("complete");
      }
    }
    this.options?.terminate?.(consumer, reason);
  }
}

type StreamFromContext<VALUE> = { pulling: boolean; consumableConsumer?: Consumer<VALUE> };
class StreamFromOptions<VALUE> extends Stream.DefaultOptions<VALUE> {
  constructor(
    private consumable: Consumable<VALUE>,
    private context: StreamFromContext<VALUE>,
    options?: Stream.Options<VALUE>,
  ) {
    super(options);
  }
  override push(stream: Stream<VALUE>, value: VALUE): void {
    this.context.pulling = false;
    this.options?.push?.(stream, value);
  }
  override next(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void {
    this.options?.next?.(stream, consumer);
    if (!this.context.pulling) {
      this.context.pulling = true;
      this.context.consumableConsumer?.next();
    }
  }
  override firstConsumerJoin(stream: Stream<VALUE>, consumer: Consumer<VALUE>): void {
    this.context.consumableConsumer = this.consumable.consume(new ConsumableConsumerOptions(stream));
    this.options?.firstConsumerJoin?.(stream, consumer);
  }
}

class ConsumableConsumerOptions<VALUE> extends Consumer.DefaultOptions<VALUE> {
  constructor(private stream: Stream<VALUE>) {
    super();
  }
  override handler(consumer: Consumer<VALUE>, value: VALUE): void | undefined {
    this.stream.push(value);
  }
  override terminate(consumer: Consumer<VALUE>, reason: TerminateReason): void {
    this.stream.terminate(reason);
  }
}
