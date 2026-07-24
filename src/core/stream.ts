import { Consumer } from "./consumer";
import type { AnyStream, Closable, NonEmptyString, Queue, Transform } from "./types";

import { Transformer } from "./transformer";

export class Stream<VALUE, NAME extends NonEmptyString = "$root"> implements Closable<NAME> {
  protected _options: Stream.Options<VALUE, NAME>;
  private _consumers: Map<Consumer.Handler<VALUE, `${NAME}Consumer`>, Consumer<VALUE, `${NAME}Consumer`>>;
  private _state: Stream.State;
  private _pulling: boolean;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    const scopeConsumer = options?.scope?.$terminate.listen((_, reason) => this.terminate(reason));
    this._options = {
      ...options,
      terminate: options?.terminate ?? ((_, reason) => scopeConsumer?.terminate(reason)),
    };

    this._consumers = new Map();
    this._state = "active";
    this._pulling = false;
  }
  get name(): NAME {
    return this._options.name ?? ("$root" as NAME);
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
  get state() {
    return this._state;
  }
  get $next() {
    return (this._options.$next ??= new Stream({
      name: `${this.name}Next`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$next = undefined;
      },
    }));
  }
  get $drain() {
    return (this._options.$drain ??= new Stream({
      name: `${this.name}Drain`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$drain = undefined;
      },
    }));
  }
  get $terminate() {
    return (this._options.$terminate ??= new Stream({
      name: `${this.name}Terminate`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$terminate = undefined;
      },
    }));
  }
  get $consumerJoin() {
    return (this._options.$consumerJoin ??= new Stream({
      name: `${this.name}ConsumerJoin`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$consumerJoin = undefined;
      },
    }));
  }
  get $consumerLeft() {
    return (this._options.$consumerLeft ??= new Stream({
      name: `${this.name}ConsumerLeft`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$consumerLeft = undefined;
      },
    }));
  }
  private _optimizePush(): void {
    const consumers = this._consumers;
    switch (consumers.size) {
      case 0:
        this.push = () => (this._pulling = false);
        break;
      case 1:
        const consumer = consumers.values().next().value!;
        this.push = (value) => {
          this._pulling = false;
          consumer.push(value);
        };
        break;
      default:
        this.push = (value) => {
          this._pulling = false;
          for (const consumer of consumers.values()) {
            consumer.push(value);
          }
        };
    }
  }
  push(value: VALUE): void {
    this._pulling = false;
  }

  listen(
    handler: Consumer.Handler<VALUE, `${NAME}Consumer`>,
    options?: Consumer.Options<VALUE, `${NAME}Consumer`>,
  ): Consumer<VALUE, `${NAME}Consumer`> {
    if (this._consumers.has(handler)) return this._consumers.get(handler)!;

    const consumer = new Consumer(handler, {
      ...options,
      name: `${this.name}Consumer`,
      queue: options?.queue ?? this._options.queueFactory?.(),
      next: (self) => {
        options?.next?.(self);
        if (this._pulling === false) {
          this._pulling = true;
          this._options.next?.(this, self);
          this._options.$next?.push(self);
        }
      },

      terminate: (self, reason) => {
        this._consumers.delete(handler);
        this._optimizePush();

        options?.terminate?.(self, reason);
        options = {};

        this._options.consumerLeft?.(this, self);
        this._options.$consumerLeft?.push(self);
        if (this._consumers.size === 0 && this._state === "drain") this.terminate("complete");
      },
    });

    this._consumers.set(handler, consumer);
    this._optimizePush();

    this._options.consumerJoin?.(this, consumer);
    this._options.$consumerJoin?.push(consumer);

    return consumer;
  }

  terminate(reason: "abort" | "complete"): void {
    this.push = () => {};
    if (reason === "abort") {
      this.terminate = () => {};
      this._state = "aborted";
    } else if (this._consumers.size) {
      this._state = "drain";
      this._options.drain?.(this);
      this._options.$drain?.push();
      return;
    } else {
      this.terminate = () => {};
      this._state = "completed";
    }

    for (const consumer of this._consumers.values()) {
      consumer.terminate(reason);
    }

    this._options.$next?.terminate(reason);
    this._options.$drain?.terminate(reason);
    this._options.$consumerJoin?.terminate(reason);
    this._options.$consumerLeft?.terminate(reason);

    this._options.terminate?.(this, reason);
    this._options.$terminate?.push(reason);
    this._options.$terminate?.terminate(reason);

    this._options = {};
  }

  pipe<OUTPUT extends Transformer<this, any, any>>(transform: Transform<this, OUTPUT>): OUTPUT {
    return transform(this);
  }
}

export namespace Stream {
  export type State = "active" | "drain" | "aborted" | "completed";

  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    scope?: AnyStream;
    queueFactory?: QueueFactory<VALUE>;
    next?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE, `${NAME}Consumer`>) => void;
    drain?: (self: Stream<VALUE, NAME>) => void;
    terminate?: (self: Stream<VALUE, NAME>, reason: "abort" | "complete") => void;
    consumerJoin?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE, `${NAME}Consumer`>) => void;
    consumerLeft?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE, `${NAME}Consumer`>) => void;
    $next?: Stream<Consumer<VALUE, `${NAME}Consumer`>, `${NAME}Next`>;
    $drain?: Stream<void, `${NAME}Drain`>;
    $terminate?: Stream<"abort" | "complete", `${NAME}Terminate`>;
    $consumerJoin?: Stream<Consumer<VALUE, `${NAME}Consumer`>, `${NAME}ConsumerJoin`>;
    $consumerLeft?: Stream<Consumer<VALUE, `${NAME}Consumer`>, `${NAME}ConsumerLeft`>;
  };
}
