import { Consumer } from "./consumer";
import type {
  NonEmptyString,
  Queue,
  Source,
  Transform,
  TerminateReason,
  AnyStream,
  Prettify,
  GetValidName,
  ExtractStream,
} from "./types";

export class Stream<VALUE, NAME extends NonEmptyString = "$root"> implements Source<VALUE> {
  private _name: NAME;
  private _options: Stream.Options<VALUE, NAME>;
  private _consumers?: Consumer<VALUE>[] | Consumer<VALUE>;
  private _status: Stream.Status;
  private _pulling: boolean;
  private _sourceConsumer?: Consumer<VALUE>;
  private _signalConsumer?: Consumer<TerminateReason>;

  private _$push?: Stream<VALUE>;
  private _$next?: Stream<Consumer<VALUE>>;
  private _$drain?: Stream<void>;
  private _$consumerJoin?: Stream<Consumer<VALUE>>;
  private _$consumerLeft?: Stream<Consumer<VALUE>>;
  private _$terminate?: Stream<TerminateReason>;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    this._options = { ...options };

    this._name = this._options.name ?? ("$root" as NAME);
    this._status = "active";
    this._pulling = false;

    this._signalConsumer = this._options.signal?.consume((_, reason) => this.terminate(reason)).next();
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
    return (this._$push ??= new Stream<VALUE>()).asSource();
  }
  get $next(): Source<Consumer<VALUE>> {
    return (this._$next ??= new Stream<Consumer<VALUE>>()).asSource();
  }
  get $drain(): Source<void> {
    return (this._$drain ??= new Stream<void>()).asSource();
  }
  get $consumerJoin(): Source<Consumer<VALUE>> {
    return (this._$consumerJoin ??= new Stream<Consumer<VALUE>>()).asSource();
  }
  get $consumerLeft(): Source<Consumer<VALUE>> {
    return (this._$consumerLeft ??= new Stream<Consumer<VALUE>>()).asSource();
  }
  get $terminate(): Source<TerminateReason> {
    return (this._$terminate ??= new Stream<TerminateReason>({
      consumerJoin: (stream, consumer) => {
        if (this._status === "abort" || this._status === "complete") {
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
      this._push = (value) => {
        this._pulling = false;
        for (let i = 0; i < length; i++) {
          snapshot[i].push(value);
        }
        this._options.push?.(this, value);
        this._$push?.push(value);
        return this;
      };
    } else if (this._consumers) {
      const consumer = this._consumers;
      this._push = (value) => {
        this._pulling = false;
        consumer.push(value);
        this._options.push?.(this, value);
        this._$push?.push(value);
        return this;
      };
    } else {
      this._push = (value: VALUE) => {
        this._pulling = false;
        this._options.push?.(this, value);
        this._$push?.push(value);
        return this;
      };
    }
  }
  private _push = (value: VALUE) => {
    this._pulling = false;
    this._options.push?.(this, value);
    this._$push?.push(value);
    return this;
  };
  push(value: VALUE): this {
    return this._push(value);
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    options = { ...options };

    const consumer = new Consumer(handler, {
      ...options,
      queue: options.queue ?? this._options.queueFactory?.(),
      next: (consumer) => {
        if (this._pulling === false) {
          this._pulling = true;
          this._sourceConsumer?.next();
          this._options.next?.(this, consumer);
          this._$next?.push(consumer);
        }
        options.next?.(consumer);
      },

      terminate: (consumer, reason) => {
        if (this._status === "active") {
          if (this._consumers === consumer) {
            this._consumers = undefined;
          } else if (Array.isArray(this._consumers)) {
            this._consumers = this._consumers.filter((consumer) => consumer !== consumer);
            if (this._consumers.length === 1) this._consumers = this._consumers[0];
          }
          this._optimizePush();
        }

        this._options.consumerLeft?.(this, consumer);
        this._$consumerLeft?.push(consumer);

        if (!this.consumersCount && this._status === "drain") this.terminate("complete");
        options.terminate?.(consumer, reason);
      },
    });

    if (!this._consumers) {
      this._consumers = consumer;
      this._sourceConsumer = this._options.source?.consume((_, value) => this.push(value));
    } else if (Array.isArray(this._consumers)) {
      this._consumers.push(consumer);
    } else {
      this._consumers = [this._consumers, consumer];
    }

    this._optimizePush();

    this._options.consumerJoin?.(this, consumer);
    this._$consumerJoin?.push(consumer);

    return consumer;
  }
  terminate(reason: TerminateReason): this {
    this.push = () => this;
    this.consume = (handler, options) => new Consumer(handler, options).terminate(reason);

    if (reason === "abort") {
      this.terminate = () => this;
      this._status = "abort";
    } else if (this.consumersCount) {
      this._status = "drain";
      this._options?.drain?.(this);
      this._$drain?.push();

      if (Array.isArray(this._consumers)) {
        for (let i = 0; i < this._consumers.length; i++) {
          this._consumers[i].terminate("complete");
        }
      } else {
        this._consumers?.terminate("complete");
      }

      return this;
    } else {
      this.terminate = () => this;
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
    this._options?.terminate?.(this, reason);
    this._$terminate?.push(reason);

    this._options = {};
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

    this.$terminate.consume((_, reason) => this.terminate(reason)).next();

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
    const self = this;
    return {
      consume(handler, options) {
        return self.consume(handler, options);
      },
    };
  }
}

export namespace Stream {
  export type Status = "active" | "drain" | TerminateReason;
  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    source?: Source<VALUE>;
    signal?: Source<TerminateReason>;
    queueFactory?: QueueFactory<VALUE>;
    push?: (stream: Stream<VALUE, NAME>, value: VALUE) => void;
    next?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    drain?: (stream: Stream<VALUE, NAME>) => void;
    terminate?: (stream: Stream<VALUE, NAME>, reason: TerminateReason) => void;
    consumerJoin?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    consumerLeft?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
  };
}
