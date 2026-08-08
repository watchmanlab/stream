import { Consumer } from "./consumer";
import type { TerminableStreamable, NonEmptyString, Queue, Source, Transform, TerminateReason } from "./types";

import { Transformer } from "./transformer";

export class Stream<VALUE, NAME extends NonEmptyString = "$root"> implements Source<VALUE>, TerminableStreamable {
  private _name: NAME;
  protected _options: Stream.Options<VALUE, NAME>;
  protected _metaStreams: Stream.MetaStreams<VALUE, NAME>;
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
    this._metaStreams = {};

    this._terminateConsumer = this._options.$terminate?.consume((self, reason) => this.terminate(reason)).next();
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
    this._metaStreams.$push ??= new Stream({ name: `${this.name}MetaPush`, $terminate: this.$terminate });
    return new Stream({ name: `${this.name}Push`, source: this._metaStreams.$push });
  }
  get $next(): Stream<Consumer<VALUE>, `${NAME}Next`> {
    this._metaStreams.$next ??= new Stream({ name: `${this.name}MetaNext`, $terminate: this.$terminate });
    return new Stream({ name: `${this.name}Next`, source: this._metaStreams.$next });
  }
  get $drain(): Stream<void, `${NAME}Drain`> {
    this._metaStreams.$drain ??= new Stream({ name: `${this.name}MetaDrain`, $terminate: this.$terminate });
    return new Stream({ name: `${this.name}Drain`, source: this._metaStreams.$drain });
  }
  get $terminate(): Stream<TerminateReason, `${NAME}Terminate`> {
    this._metaStreams.$terminate ??= new Stream({ name: `${this.name}MetaTerminate` }); // will be terminated manually to avoid circular refecrence
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
    this._metaStreams.$consumerJoin ??= new Stream({
      name: `${this.name}MetaConsumerJoin`,
      $terminate: this.$terminate,
    });
    return new Stream({ name: `${this.name}ConsumerJoin`, source: this._metaStreams.$consumerJoin });
  }
  get $consumerLeft(): Stream<Consumer<VALUE>, `${NAME}ConsumerLeft`> {
    this._metaStreams.$consumerLeft ??= new Stream({
      name: `${this.name}MetaConsumerLeft`,
      $terminate: this.$terminate,
    });
    return new Stream({ name: `${this.name}ConsumerLeft`, source: this._metaStreams.$consumerLeft });
  }
  private _optimizePush(): void {
    const consumers = this._consumers;
    switch (consumers.size) {
      case 0:
        {
          this._push = (value) => {
            this._pulling = false;
            this._options.push?.(this, value);
            this._metaStreams.$push?.push(value);
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
            this._metaStreams.$push?.push(value);
            return this;
          };
        }
        break;
      default: {
        const snapshot = [...consumers.values()];
        this._push = (value) => {
          this._pulling = false;
          for (let i = 0; i < snapshot.length; i++) {
            snapshot[i].push(value);
          }
          this._options.push?.(this, value);
          this._metaStreams.$push?.push(value);
          return this;
        };
      }
    }
  }
  private _push = (value: VALUE) => {
    this._pulling = false;
    this._options.push?.(this, value);
    this._metaStreams.$push?.push(value);
    return this;
  };
  push(value: VALUE): this {
    return this._push(value);
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    if (this._consumers.has(handler)) return this._consumers.get(handler)!;

    if (this._consumers.size === 0)
      this._sourceConsumer = this._options.source?.consume((_, value) => this.push(value), {
        terminate: (_, reason) => this.terminate(reason),
      });
    options = { ...options };

    const consumer = new Consumer(handler, {
      ...options,
      queue: options.queue ?? this._options.queueFactory?.(),
      next: (self) => {
        options.next?.(self);
        if (this._pulling === false) {
          this._pulling = true;
          this._sourceConsumer?.next();
          this._options.next?.(this, self);
          this._metaStreams.$next?.push(self);
        }
      },

      terminate: (self, reason) => {
        this._consumers.delete(handler);
        if (this._status == "active") this._optimizePush();
        if (this._consumers.size === 0) this._sourceConsumer?.terminate("complete");

        this._options.consumerLeft?.(this, self);
        this._metaStreams.$consumerLeft?.push(self);
        if (this._consumers.size === 0 && this._status === "drain") this.terminate("complete");
        options.terminate?.(self, reason);
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
    this._sourceConsumer?.terminate(reason);
    this._terminateConsumer?.terminate(reason);
    this._metaStreams?.$terminate?.push(reason);
    this._metaStreams?.$terminate?.terminate(reason);
    this._options?.terminate?.(this, reason);

    this._options = {};
    this._metaStreams = {};
    this._sourceConsumer = this._terminateConsumer = undefined;
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
  export type MetaStreams<VALUE, NAME extends NonEmptyString> = {
    $push?: Stream<VALUE, `${NAME}MetaPush`>;
    $next?: Stream<Consumer<VALUE>, `${NAME}MetaNext`>;
    $drain?: Stream<void, `${NAME}MetaDrain`>;
    $terminate?: Stream<TerminateReason, `${NAME}MetaTerminate`>;
    $consumerJoin?: Stream<Consumer<VALUE>, `${NAME}MetaConsumerJoin`>;
    $consumerLeft?: Stream<Consumer<VALUE>, `${NAME}MetaConsumerLeft`>;
  };
}
