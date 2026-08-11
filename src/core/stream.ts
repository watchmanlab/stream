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
  private _consumers: Map<Consumer.Handler<VALUE>, Consumer<VALUE>>;
  private _status: Stream.Status;
  private _pulling: boolean;
  private _sourceConsumer?: Consumer<VALUE>;
  private _terminateConsumer?: Consumer<TerminateReason>;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    this._options = { ...options };

    this._name = this._options.name ?? ("$root" as NAME);
    this._consumers = new Map();
    this._status = "active";
    this._pulling = false;

    this._terminateConsumer = this._options.signal?.consume((_, reason) => this.terminate(reason)).next();
  }
  get name(): NAME {
    return this._name;
  }
  get consumersCount() {
    return this._consumers.size;
  }
  get status(): Stream.Status {
    return this._status;
  }

  private _optimizePush(): void {
    const consumers = this._consumers;
    switch (consumers.size) {
      case 0:
        {
          this._push = (value) => {
            this._pulling = false;
            this._options.push?.(this, value);
            return this;
          };
        }
        break;
      case 1:
        {
          const consumer = consumers.values().next().value!;

          this._push = (value) => {
            this._pulling = false;
            consumer.push(value);
            this._options.push?.(this, value);
            return this;
          };
        }
        break;
      default: {
        const snapshot = Array.from(consumers.values());
        this._push = (value) => {
          this._pulling = false;
          for (let i = 0; i < snapshot.length; i++) {
            snapshot[i].push(value);
          }
          this._options.push?.(this, value);
          return this;
        };
      }
    }
  }
  private _push = (value: VALUE) => {
    this._pulling = false;
    this._options.push?.(this, value);

    return this;
  };
  push(value: VALUE): this {
    return this._push(value);
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    if (this._consumers.has(handler)) return this._consumers.get(handler)!;

    if (this._consumers.size === 0)
      this._sourceConsumer = this._options.source?.consume((_, value) => this.push(value));

    options = { ...options };

    const consumer = new Consumer(handler, {
      ...options,
      queue: options.queue ?? this._options.queueFactory?.(),
      next: (self) => {
        if (this._pulling === false) {
          this._pulling = true;
          this._sourceConsumer?.next();
          this._options.next?.(this, self);
        }
        options.next?.(self);
      },

      terminate: (self, reason) => {
        this._consumers.delete(handler);
        if (this._status == "active") this._optimizePush();

        this._options.consumerLeft?.(this, self);
        if (this._consumers.size === 0 && this._status === "drain") this.terminate("complete");
        options.terminate?.(self, reason);
      },
    });

    this._consumers.set(handler, consumer);
    this._optimizePush();

    this._options.consumerJoin?.(this, consumer);

    return consumer;
  }
  terminate(reason: TerminateReason): this {
    this.push = () => this;
    this.consume = (handler, options) => new Consumer(handler, options).terminate(reason);

    if (reason === "abort") {
      this.terminate = () => this;
      this._status = "abort";
    } else if (this._consumers.size) {
      this._status = "drain";
      this._options?.drain?.(this);
      for (const consumer of this._consumers.values()) {
        consumer.terminate(reason);
      }

      return this;
    } else {
      this.terminate = () => this;
      this._status = "complete";
    }
    for (const consumer of this._consumers.values()) {
      consumer.terminate(reason);
    }
    this._sourceConsumer?.terminate(reason);
    this._terminateConsumer?.terminate(reason);
    this._options?.terminate?.(this, reason);

    this._options = {};
    this._sourceConsumer = this._terminateConsumer = undefined;
    return this;
  }
  pipe<OUTPUT_NAME extends NonEmptyString, OUTPUT extends Stream<any, OUTPUT_NAME>>(
    transform: Transform<this, OUTPUT_NAME, OUTPUT>,
  ): ExtractStream<OUTPUT> & Prettify<Omit<OUTPUT, keyof AnyStream> & Record<GetValidName<NAME, OUTPUT, 5>, this>> {
    const output = transform(this) as any;

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
}

export namespace Stream {
  export type Status = "active" | "drain" | TerminateReason;
  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    source?: Source<VALUE>;
    signal?: Stream<TerminateReason, any>;
    queueFactory?: QueueFactory<VALUE>;
    push?: (self: Stream<VALUE, NAME>, value: VALUE) => void;
    next?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    drain?: (self: Stream<VALUE, NAME>) => void;
    terminate?: (self: Stream<VALUE, NAME>, reason: TerminateReason) => void;
    consumerJoin?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    consumerLeft?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
  };
}
