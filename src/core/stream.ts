import { Consumer } from "./consumer";
import type { AnyStream, Closable, NonEmptyString, Queue, Source, Transform } from "./types";

import { Transformer } from "./transformer";

export class Stream<VALUE, NAME extends NonEmptyString = "$root"> implements Source<VALUE>, Closable<NAME> {
  protected _options: Stream.Options<any, any>;
  private _consumers: Map<Consumer.Handler<VALUE>, Consumer<VALUE>>;
  private _state: Stream.State;
  private _pulling: boolean;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    const _options = { ...options };

    const scopeConsumer = _options.scope?.$terminate.listen((_, reason) => this.terminate(reason));
    const sourceConsumer = _options.source?.listen((_, value) => this.push(value), {
      terminate: (_, reason) => this.terminate(reason),
    });

    this._options = {
      ..._options,
      next: (self, consumer) => {
        _options.next?.(self, consumer);
        sourceConsumer?.next();
      },
      terminate: (self, reason) => {
        sourceConsumer?.terminate(reason);
        scopeConsumer?.terminate(reason);
        _options.terminate?.(self, reason);
      },
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
  get $next(): Stream<Consumer<VALUE>, `${NAME}Next`> {
    this._options.$next ??= new Stream();
    return new Stream({ name: `${this.name}Next`, source: this._options.$next });
  }
  get $drain(): Stream<void, `${NAME}Drain`> {
    this._options.$drain ??= new Stream();
    return new Stream({ name: `${this.name}Drain`, source: this._options.$drain });
  }
  get $terminate(): Stream<"abort" | "complete", `${NAME}Terminate`> {
    this._options.$terminate ??= new Stream();
    return new Stream({ name: `${this.name}Terminate`, source: this._options.$terminate });
  }
  get $consumerJoin(): Stream<Consumer<VALUE>, `${NAME}ConsumerJoin`> {
    this._options.$consumerJoin ??= new Stream();
    return new Stream({ name: `${this.name}ConsumerJoin`, source: this._options.$consumerJoin });
  }
  get $consumerLeft(): Stream<Consumer<VALUE>, `${NAME}ConsumerLeft`> {
    this._options.$consumerLeft ??= new Stream();
    return new Stream({ name: `${this.name}ConsumerLeft`, source: this._options.$consumerLeft });
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

    this._options.$terminate?.push(reason);
    this._options.$terminate?.terminate(reason);
    this._options.terminate?.(this, reason);

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
    scope?: Closable<any>;
    source?: Source<VALUE>;
    queueFactory?: QueueFactory<VALUE>;
    next?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    drain?: (self: Stream<VALUE, NAME>) => void;
    terminate?: (self: Stream<VALUE, NAME>, reason: "abort" | "complete") => void;
    consumerJoin?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    consumerLeft?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE>) => void;
    $next?: Stream<Consumer<VALUE>, any>;
    $drain?: Stream<void, any>;
    $terminate?: Stream<"abort" | "complete", any>;
    $consumerJoin?: Stream<Consumer<VALUE>, any>;
    $consumerLeft?: Stream<Consumer<VALUE>, any>;
  };
}
