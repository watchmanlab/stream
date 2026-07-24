import { Consumer } from "./consumer";
import type { AnyStream, Closable, Queue, Transform } from "./types";

import { Transformer } from "./transformer";

export class Stream<VALUE> implements Closable {
  private _options: Stream.Options<VALUE>;
  private _consumers: Map<Consumer.Handler<VALUE>, Consumer<VALUE>>;
  private _state: Stream.State;
  private _pulling: boolean;

  constructor(options?: Stream.Options<VALUE>) {
    const scopeConsumer = options?.scope?.$terminate.listen((_, reason) => this.terminate(reason));
    this._options = {
      ...options,
      terminate: options?.terminate ?? ((_, reason) => scopeConsumer?.terminate(reason)),
    };
    this._consumers = new Map();
    this._state = "active";
    this._pulling = false;
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
  get $next(): Stream<Consumer<VALUE>> {
    return (this._options.$next ??= new Stream({
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$next = undefined;
      },
    }));
  }
  get $drain(): Stream<void> {
    return (this._options.$drain ??= new Stream({
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$drain = undefined;
      },
    }));
  }
  get $terminate(): Stream<"abort" | "complete"> {
    return (this._options.$terminate ??= new Stream({
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$terminate = undefined;
      },
    }));
  }
  get $consumerJoin(): Stream<Consumer<VALUE>> {
    return (this._options.$consumerJoin ??= new Stream({
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$consumerJoin = undefined;
      },
    }));
  }
  get $consumerLeft(): Stream<Consumer<VALUE>> {
    return (this._options.$consumerLeft ??= new Stream({
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

  listen(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    if (this._consumers.has(handler)) return this._consumers.get(handler)!;

    const consumer = new Consumer(handler, {
      ...options,
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

  pipe<OUTPUT extends Transformer<this, any>>(transform: Transform<this, OUTPUT>): OUTPUT {
    return transform(this);
  }
}

export namespace Stream {
  export type State = "active" | "drain" | "aborted" | "completed";

  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE> = {
    scope?: AnyStream;
    queueFactory?: QueueFactory<VALUE>;
    next?: (self: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    drain?: (self: Stream<VALUE>) => void;
    terminate?: (self: Stream<VALUE>, reason: "abort" | "complete") => void;
    consumerJoin?: (self: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    consumerLeft?: (self: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    $next?: Stream<Consumer<VALUE>>;
    $drain?: Stream<void>;
    $terminate?: Stream<"abort" | "complete">;
    $consumerJoin?: Stream<Consumer<VALUE>>;
    $consumerLeft?: Stream<Consumer<VALUE>>;
  };
}
