import { Consumer } from "./consumer";
import type {
  TerminableStreamable,
  NonEmptyString,
  Queue,
  Source,
  Transform,
  AnyStream,
  TerminateReason,
} from "./types";

import { Transformer } from "./transformer";

export class Stream<VALUE, NAME extends NonEmptyString = "$root"> implements Source<VALUE>, TerminableStreamable {
  private _name: NAME;
  protected _options: Stream.Options<VALUE, NAME>;
  private _consumers: Map<Consumer.Handler<VALUE>, Consumer<VALUE>>;
  private _status: Stream.Status;
  private _pulling: boolean;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    options = { ...options };

    this._name = options.name ?? ("$root" as NAME);
    this._consumers = new Map();
    this._status = "active";
    this._pulling = false;

    let terminateReason: TerminateReason | undefined = undefined;

    const scopeConsumers: Consumer<TerminateReason>[] = [];

    for (const stream of [...new Set(options.scope)]) {
      if (stream.status === "abort" || stream.status === "complete") {
        terminateReason = stream.status;
        break;
      }
      scopeConsumers.push(stream.$terminate.consume((_, reason) => this.terminate(reason)).next());
    }

    let sourceConsumer = options.source?.consume((_, value) => this.push(value), {
      terminate: (_, reason) => this.terminate(reason),
    });

    this._options = terminateReason
      ? {}
      : {
          ...options,
          next: (self, consumer) => {
            options.next?.(self, consumer);
            sourceConsumer?.next();
          },
          terminate: (self, reason) => {
            scopeConsumers.forEach((consumer) => consumer.terminate(reason));
            scopeConsumers.length = 0;
            sourceConsumer?.terminate(reason);
            sourceConsumer = undefined;
            options.terminate?.(self, reason);
          },
        };

    if (terminateReason) {
      this._status = terminateReason;
      this.push = this.terminate = () => this;
      this.consume = (handler, options) => new Consumer(handler, options).terminate(terminateReason!);
      scopeConsumers.forEach((consumer) => consumer.terminate(terminateReason!));
      scopeConsumers.length = 0;
    }
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
    this._options.$push ??= new Stream({ scope: [this] });
    return new Stream({ name: `${this.name}Push`, source: this._options.$push });
  }
  get $next(): Stream<Consumer<VALUE>, `${NAME}Next`> {
    this._options.$next ??= new Stream({ scope: [this] });
    return new Stream({ name: `${this.name}Next`, source: this._options.$next });
  }
  get $drain(): Stream<void, `${NAME}Drain`> {
    this._options.$drain ??= new Stream({ scope: [this] });
    return new Stream({ name: `${this.name}Drain`, source: this._options.$drain });
  }
  get $terminate(): Stream<TerminateReason, `${NAME}Terminate`> {
    this._options.$terminate ??= new Stream(); // will be terminated manually to avoid circular termination
    return new Stream({
      name: `${this.name}Terminate`,
      source: this._options.$terminate,
      consumerJoin: (self, consumer) => {
        if (this.status === "abort" || this.status === "complete") {
          consumer.push(this.status);
          consumer.terminate("complete");
        }
      },
    });
  }
  get $consumerJoin(): Stream<Consumer<VALUE>, `${NAME}ConsumerJoin`> {
    this._options.$consumerJoin ??= new Stream({ scope: [this] });
    return new Stream({ name: `${this.name}ConsumerJoin`, source: this._options.$consumerJoin });
  }
  get $consumerLeft(): Stream<Consumer<VALUE>, `${NAME}ConsumerLeft`> {
    this._options.$consumerLeft ??= new Stream({ scope: [this] });
    return new Stream({ name: `${this.name}ConsumerLeft`, source: this._options.$consumerLeft });
  }
  private _optimizePush(): void {
    const consumers = this._consumers;
    switch (consumers.size) {
      case 0:
        this.push = (value) => {
          this._pulling = false;
          this._options.push?.(this, value);
          this._options.$push?.push(value);
          return this;
        };
        break;
      case 1:
        const consumer = consumers.values().next().value!;
        this.push = (value) => {
          this._pulling = false;
          this._options.push?.(this, value);
          this._options.$push?.push(value);
          consumer.push(value);
          return this;
        };
        break;
      default:
        this.push = (value) => {
          this._pulling = false;
          for (const consumer of consumers.values()) {
            this._options.push?.(this, value);
            this._options.$push?.push(value);
            consumer.push(value);
          }
          return this;
        };
    }
  }
  push(value: VALUE): this {
    this._pulling = false;
    this._options.push?.(this, value);
    this._options.$push?.push(value);
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
          this._options.$next?.push(self);
        }
      },

      terminate: (self, reason) => {
        this._consumers.delete(handler);
        this._optimizePush();

        _options.terminate?.(self, reason);
        _options = {};

        this._options.consumerLeft?.(this, self);
        this._options.$consumerLeft?.push(self);
        if (this._consumers.size === 0 && this._status === "drain") this.terminate("complete");
      },
    });

    this._consumers.set(handler, consumer);
    this._optimizePush();

    this._options.consumerJoin?.(this, consumer);
    this._options.$consumerJoin?.push(consumer);

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
      this._options?.$drain?.push();
      for (const consumer of this._consumers.values()) {
        consumer.terminate(reason);
      }

      return this;
    } else {
      this.terminate = () => this;
      this._status = "complete";
    }
    this._options?.$terminate?.push(reason);
    this._options?.$terminate?.terminate(reason); // terminate it even if it's not owned
    this._options?.terminate?.(this, reason);

    this._options = {};
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
    scope?: [AnyStream, ...AnyStream[]];
    source?: Source<VALUE>;
    queueFactory?: QueueFactory<VALUE>;
    push?: (self: Stream<VALUE, NAME>, value: VALUE) => void;
    next?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    drain?: (self: Stream<VALUE, NAME>) => void;
    terminate?: (self: Stream<VALUE, NAME>, reason: TerminateReason) => void;
    consumerJoin?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    consumerLeft?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    $push?: Stream<VALUE, any>;
    $next?: Stream<Consumer<VALUE>, any>;
    $drain?: Stream<void, any>;
    $terminate?: Stream<TerminateReason, any>;
    $consumerJoin?: Stream<Consumer<VALUE>, any>;
    $consumerLeft?: Stream<Consumer<VALUE>, any>;
  };
}
