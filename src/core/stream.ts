import { Consumer } from "./consumer";
import type {
  TerminableStreamable,
  NonEmptyString,
  Queue,
  Source,
  Transform,
  TerminateReason,
  AnyStream,
} from "./types";

import { Transformer } from "./transformer";

export class Stream<VALUE, NAME extends NonEmptyString = "$root"> implements Source<VALUE>, TerminableStreamable {
  private _name: NAME;
  protected _options: Stream.Options<VALUE, NAME>;
  protected _metaStreams: Stream.MetaStreams<VALUE>;
  private _consumers: Map<Consumer.Handler<VALUE>, Consumer<VALUE>>;
  private _status: Stream.Status;
  private _pulling: boolean;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    options = { ...options };

    this._name = options.name ?? ("$root" as NAME);
    this._consumers = new Map();
    this._status = "active";
    this._pulling = false;
    this._metaStreams = {};

    let sourceConsumer = options.source?.consume((_, value) => this.push(value), {
      terminate: (_, reason) => this.terminate(reason),
    });

    this._options = {
      ...options,
      next: (self, consumer) => {
        options.next?.(self, consumer);
        sourceConsumer?.next();
      },
      terminate: (self, reason) => {
        sourceConsumer?.terminate(reason);
        terminateConsumer?.terminate(reason);
        sourceConsumer = terminateConsumer = undefined;
        options.terminate?.(self, reason);
      },
    };

    let terminateConsumer = options.$terminate?.consume((self, reason) => this.terminate(reason)).next();
  }
  get name(): NAME {
    return this._name;
  }
  get consumers() {
    const self = this;
    return {
      get count() {
        return self._consumers.size;
      },
      get handlers() {
        return self._consumers.keys();
      },
      [Symbol.iterator]() {
        return self._consumers.values();
      },
    };
  }
  get status(): Stream.Status {
    return this._status;
  }
  get $push(): Stream<VALUE, `${NAME}Push`> {
    this._metaStreams.$push ??= new Stream({ $terminate: this.$terminate });
    return new Stream({ name: `${this.name}Push`, source: this._metaStreams.$push });
  }
  get $next(): Stream<Consumer<VALUE>, `${NAME}Next`> {
    this._metaStreams.$next ??= new Stream({ $terminate: this.$terminate });
    return new Stream({ name: `${this.name}Next`, source: this._metaStreams.$next });
  }
  get $drain(): Stream<void, `${NAME}Drain`> {
    this._metaStreams.$drain ??= new Stream({ $terminate: this.$terminate });
    return new Stream({ name: `${this.name}Drain`, source: this._metaStreams.$drain });
  }
  get $terminate(): Stream<TerminateReason, `${NAME}Terminate`> {
    this._metaStreams.$terminate ??= new Stream(); // will be terminated manually to avoid circular refecrence
    return new Stream({
      name: `${this.name}Terminate`,
      source: this._metaStreams.$terminate,
      consumerJoin: (self, consumer) => {
        if (this.status === "abort" || this.status === "complete") {
          consumer.push(this.status);
          consumer.terminate("complete");
        }
      },
    });
  }
  get $consumerJoin(): Stream<Consumer<VALUE>, `${NAME}ConsumerJoin`> {
    this._metaStreams.$consumerJoin ??= new Stream({ $terminate: this.$terminate });
    return new Stream({ name: `${this.name}ConsumerJoin`, source: this._metaStreams.$consumerJoin });
  }
  get $consumerLeft(): Stream<Consumer<VALUE>, `${NAME}ConsumerLeft`> {
    this._metaStreams.$consumerLeft ??= new Stream({ $terminate: this.$terminate });
    return new Stream({ name: `${this.name}ConsumerLeft`, source: this._metaStreams.$consumerLeft });
  }
  private _optimizePush(): void {
    const consumers = this._consumers;
    switch (consumers.size) {
      case 0:
        this.push = (value) => {
          this._pulling = false;
          this._options.push?.(this, value);
          this._metaStreams.$push?.push(value);
          return this;
        };
        break;
      case 1:
        const consumer = consumers.values().next().value!;
        this.push = (value) => {
          this._pulling = false;
          this._options.push?.(this, value);
          this._metaStreams.$push?.push(value);
          consumer.push(value);
          return this;
        };
        break;
      default:
        this.push = (value) => {
          this._pulling = false;
          this._options.push?.(this, value);
          this._metaStreams.$push?.push(value);
          for (const consumer of consumers.values()) {
            consumer.push(value);
          }
          return this;
        };
    }
  }
  push(value: VALUE): this {
    this._pulling = false;
    this._options.push?.(this, value);
    this._metaStreams.$push?.push(value);
    return this;
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    if (this._consumers.has(handler)) return this._consumers.get(handler)!;

    let _options = { ...options };

    const consumer = new Consumer(handler, {
      ..._options,
      queue: _options.queue ?? this._options.queueFactory?.(),
      next: (self) => {
        _options.next?.(self);
        if (this._pulling === false) {
          this._pulling = true;
          this._options.next?.(this, self);
          this._metaStreams.$next?.push(self);
        }
      },

      terminate: (self, reason) => {
        this._consumers.delete(handler);
        this._optimizePush();

        _options.terminate?.(self, reason);
        _options = {};

        this._options.consumerLeft?.(this, self);
        this._metaStreams.$consumerLeft?.push(self);
        if (this._consumers.size === 0 && this._status === "drain") this.terminate("complete");
      },
    });

    this._consumers.set(handler, consumer);
    this._optimizePush();

    this._options.consumerJoin?.(this, consumer);
    this._metaStreams.$consumerJoin?.push(consumer);

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
      this._metaStreams?.$drain?.push();
      for (const consumer of this._consumers.values()) {
        consumer.terminate(reason);
      }

      return this;
    } else {
      this.terminate = () => this;
      this._status = "complete";
    }
    this._metaStreams?.$terminate?.push(reason);
    this._metaStreams?.$terminate?.terminate(reason);
    this._options?.terminate?.(this, reason);

    this._options = {};
    this._metaStreams = {};
    return this;
  }
  pipe<OUTPUT extends Transformer<this, any, any> | this>(transform: Transform<this, OUTPUT>): OUTPUT {
    return transform(this);
  }
}

export namespace Stream {
  export type Status = "active" | "drain" | TerminateReason;
  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    source?: Source<VALUE>;
    $terminate?: Stream<TerminateReason, any>;
    queueFactory?: QueueFactory<VALUE>;
    push?: (self: Stream<VALUE, NAME>, value: VALUE) => void;
    next?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    drain?: (self: Stream<VALUE, NAME>) => void;
    terminate?: (self: Stream<VALUE, NAME>, reason: TerminateReason) => void;
    consumerJoin?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    consumerLeft?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
  };
  export type MetaStreams<VALUE> = {
    $push?: Stream<VALUE>;
    $next?: Stream<Consumer<VALUE>>;
    $drain?: Stream<void>;
    $terminate?: Stream<TerminateReason>;
    $consumerJoin?: Stream<Consumer<VALUE>>;
    $consumerLeft?: Stream<Consumer<VALUE>>;
  };
}
