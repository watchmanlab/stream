import { Consumer } from "./consumer";
import {
  type NonEmptyString,
  type Queue,
  type Source,
  type Transform,
  type TerminateReason,
  type AnyStream,
  type Prettify,
  type GetValidName,
  type ExtractStream,
  EMPTY_THIS_FUNCTION,
  EMPTY_FUNCTION,
  Terminable,
} from "./types";

export class Stream<VALUE, NAME extends NonEmptyString = "$root"> implements Source<VALUE>, Terminable {
  private _name: NAME;
  private _source?: Source<VALUE>;

  private _queueFactory?: () => Queue<VALUE>;
  private _push: (stream: Stream<VALUE, NAME>, value: VALUE) => void;
  private _next: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
  private _consumerJoin: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
  private _consumerLeft: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
  private _firstConsumerJoin: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
  private _lastConsumerLeft: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
  private _drain: (stream: Stream<VALUE, NAME>) => void;
  private _terminate: (stream: Stream<VALUE, NAME>, reason: TerminateReason) => void;

  // Events
  private _$push?: Stream<VALUE>;
  private _$next?: Stream<Consumer<VALUE>>;
  private _$drain?: Stream<void>;
  private _$consumerJoin?: Stream<Consumer<VALUE>>;
  private _$consumerLeft?: Stream<Consumer<VALUE>>;
  private _$firstConsumerJoin?: Stream<Consumer<VALUE>>;
  private _$lastConsumerLeft?: Stream<Consumer<VALUE>>;
  private _$terminate?: Stream<TerminateReason>;

  private _consumers?: Consumer<VALUE>[] | Consumer<VALUE>;
  private _status: Stream.Status;
  private _pulling: boolean;
  private _sourceConsumer?: Consumer<VALUE>;
  private _signalConsumer?: Consumer<TerminateReason>;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    const {
      name,
      source,
      signal,
      queueFactory,
      push,
      next,
      consumerJoin,
      consumerLeft,
      firstConsumerJoin,
      lastConsumerLeft,
      drain,
      terminate,
    } = options ?? {};

    this._name = name ?? ("$root" as NAME);
    this._source = source;

    this._queueFactory = queueFactory;

    this._push = push
      ? (stream, value) => (push(stream, value), this._$push?.push(value))
      : (_, value) => this._$push?.push(value);

    this._next = next
      ? (stream, consumer) => (next(stream, consumer), this._$next?.push(consumer))
      : (_, consumer) => this._$next?.push(consumer);

    this._consumerJoin = consumerJoin
      ? (stream, consumer) => (consumerJoin(stream, consumer), this._$consumerJoin?.push(consumer))
      : (_, consumer) => this._$consumerJoin?.push(consumer);

    this._consumerLeft = consumerLeft
      ? (stream, consumer) => (consumerLeft(stream, consumer), this._$consumerLeft?.push(consumer))
      : (_, consumer) => this._$consumerLeft?.push(consumer);

    this._firstConsumerJoin = firstConsumerJoin
      ? (stream, consumer) => (firstConsumerJoin(stream, consumer), this._$firstConsumerJoin?.push(consumer))
      : (_, consumer) => this._$firstConsumerJoin?.push(consumer);

    this._lastConsumerLeft = lastConsumerLeft
      ? (stream, consumer) => (lastConsumerLeft(stream, consumer), this._$lastConsumerLeft?.push(consumer))
      : (_, consumer) => this._$lastConsumerLeft?.push(consumer);

    this._drain = drain ? (stream) => (drain(stream), this._$drain?.push()) : (_) => this._$drain?.push();

    this._terminate = terminate
      ? (stream, reason) => (terminate(stream, reason), this._$terminate?.push(reason))
      : (_, reason) => this._$terminate?.push(reason);

    this._status = "active";
    this._pulling = false;

    this._signalConsumer = signal?.consume((_, reason) => this.terminate(reason)).next();
  }
  get name(): NAME {
    return this._name;
  }
  get consumersCount(): number {
    return this._consumers instanceof Consumer ? 1 : (this._consumers?.length ?? 0);
  }
  get status(): Stream.Status {
    return this._status;
  }
  get $push(): Source<VALUE> {
    return (this._$push ??= new Stream<VALUE>({
      lastConsumerLeft: () => (this._$push = undefined),
    })).asSource();
  }
  get $next(): Source<Consumer<VALUE>> {
    return (this._$next ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._$next = undefined),
    })).asSource();
  }
  get $consumerJoin(): Source<Consumer<VALUE>> {
    return (this._$consumerJoin ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._$consumerJoin = undefined),
    })).asSource();
  }
  get $consumerLeft(): Source<Consumer<VALUE>> {
    return (this._$consumerLeft ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._$consumerLeft = undefined),
    })).asSource();
  }
  get $firstConsumerJoin(): Source<Consumer<VALUE>> {
    return (this._$firstConsumerJoin ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._$firstConsumerJoin = undefined),
    })).asSource();
  }
  get $lastConsumerLeft(): Source<Consumer<VALUE>> {
    return (this._$lastConsumerLeft ??= new Stream<Consumer<VALUE>>({
      lastConsumerLeft: () => (this._$lastConsumerLeft = undefined),
    })).asSource();
  }
  get $drain(): Source<void> {
    return (this._$drain ??= new Stream<void>({
      lastConsumerLeft: () => (this._$drain = undefined),
    })).asSource();
  }
  get $terminate(): Source<TerminateReason> {
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

  private _optimizePush(): void {
    if (Array.isArray(this._consumers)) {
      const snapshot = Array.from(this._consumers);
      const length = snapshot.length;
      this._optimizedPush = (value) => {
        this._pulling = false;
        for (let i = 0; i < length; i++) {
          snapshot[i].push(value);
        }
        this._push(this, value);

        return this;
      };
    } else if (this._consumers) {
      const consumer = this._consumers;
      this._optimizedPush = (value) => {
        this._pulling = false;
        consumer.push(value);
        this._push(this, value);

        return this;
      };
    } else {
      this._optimizedPush = (value: VALUE) => {
        this._pulling = false;
        this._push(this, value);
        return this;
      };
    }
  }
  private _optimizedPush = (value: VALUE) => {
    this._pulling = false;
    this._push(this, value);
    return this;
  };
  push(value: VALUE): this {
    return this._optimizedPush(value);
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    options = { ...options };

    const consumer = new Consumer(handler, {
      ...options,
      queueFactory: options.queueFactory ?? this._queueFactory,
      next: (consumer) => {
        if (this._pulling === false) {
          this._pulling = true;
          this._sourceConsumer?.next();
          this._next(this, consumer);
        }
        options.next?.(consumer);
      },

      terminate: (consumer, reason) => {
        if (this._status === "active") {
          if (this._consumers === consumer) {
            this._consumers = undefined;
          } else if (Array.isArray(this._consumers)) {
            this._consumers = this._consumers.filter((item) => item !== consumer);
            if (this._consumers.length === 1) this._consumers = this._consumers[0];
          }
          this._optimizePush();
        }

        this._consumerLeft(this, consumer);

        if (!this.consumersCount) {
          this._lastConsumerLeft(this, consumer);
          if (this._status === "drain") this.terminate("complete");
        }
        options.terminate?.(consumer, reason);
      },
    });

    if (!this._consumers) {
      this._consumers = consumer;
      this._firstConsumerJoin(this, consumer);
      this._sourceConsumer = this._source?.consume((_, value) => this.push(value));
    } else if (Array.isArray(this._consumers)) {
      this._consumers.push(consumer);
    } else {
      this._consumers = [this._consumers, consumer];
    }

    this._optimizePush();

    this._consumerJoin(this, consumer);

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
      this._drain(this);

      if (Array.isArray(this._consumers)) {
        for (let i = 0; i < this._consumers.length; i++) {
          this._consumers[i].terminate("complete");
        }
      } else {
        this._consumers?.terminate("complete");
      }
      this._consumers = undefined;

      return this;
    } else {
      this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "complete";
    }
    if (Array.isArray(this._consumers)) {
      for (let i = 0; i < this._consumers.length; i++) {
        this._consumers[i].terminate(reason);
      }
    } else {
      this._consumers?.terminate(reason);
    }

    this._sourceConsumer?.terminate(reason);
    this._signalConsumer?.terminate(reason);
    this._$push?.terminate(reason);
    this._$next?.terminate(reason);
    this._$drain?.terminate(reason);
    this._$consumerJoin?.terminate(reason);
    this._$consumerLeft?.terminate(reason);
    this._terminate(this, reason);
    this._$terminate?.terminate(reason);

    this._push = this._next = this._drain = this._consumerJoin = this._consumerLeft = this._terminate = EMPTY_FUNCTION;

    this._source =
      this._queueFactory =
      this._consumers =
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
  pipe<OUTPUT_NAME extends NonEmptyString, OUTPUT extends Stream<any, OUTPUT_NAME>>(
    transform: Transform<this, OUTPUT_NAME, OUTPUT>,
  ): ExtractStream<OUTPUT> & Prettify<Omit<OUTPUT, keyof AnyStream> & Record<GetValidName<NAME, OUTPUT, 5>, this>> {
    const output = transform(this) as any;

    // this.$terminate.consume((_, reason) => output.terminate(reason)).next();

    const getValidName = (name: string, retry: number) => {
      if (--retry === 0)
        throw new Error(
          `The output stream "${output.name}" has the property "${this.name}" which will be overridden by the input stream with the same name.
          Try to change the input stream name`,
        );

      if (name in output) return getValidName(`$${name}`, retry);

      return name;
    };

    return Object.assign(output, { [getValidName(this.name, 5)]: this });
  }
  asSource(): Source<VALUE> {
    return {
      consume: (handler, options) => {
        return this.consume(handler, options);
      },
    };
  }
}

export namespace Stream {
  export type Status = "active" | "drain" | TerminateReason;

  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    source?: Source<VALUE>;
    signal?: Source<TerminateReason>;
    queueFactory?: () => Queue<VALUE>;
    push?: (stream: Stream<VALUE, NAME>, value: VALUE) => void;
    next?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    consumerJoin?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    consumerLeft?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    firstConsumerJoin?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    lastConsumerLeft?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    drain?: (stream: Stream<VALUE, NAME>) => void;
    terminate?: (stream: Stream<VALUE, NAME>, reason: TerminateReason) => void;
  };
}
