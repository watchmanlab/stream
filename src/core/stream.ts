import { Consumer } from "./consumer";
import type { AnyStream, Closable, NonEmptyString, Queue, Source, Transform } from "./types";
import { ScopeBinder } from "./scope-binder";
import { Transformer } from "./transformer";

export class Stream<VALUE, NAME extends NonEmptyString = "$root", SELF = never> implements Source<VALUE>, Closable {
  private _name: NAME;
  private _consumers: Map<Consumer.Handler<VALUE, `${NAME}Consumer`>, Consumer<VALUE, `${NAME}Consumer`>>;
  private _state: Stream.State;
  private _scopeBinder?: ScopeBinder;
  private _queueFactory?: Stream.QueueFactory<VALUE>;

  private _next: NonNullable<Stream.Options<VALUE, NAME>["next"]>;
  private _drain: NonNullable<Stream.Options<VALUE, NAME>["drain"]>;
  private _terminate: NonNullable<Stream.Options<VALUE, NAME>["terminate"]>;
  private _consumerJoin: NonNullable<Stream.Options<VALUE, NAME>["consumerJoin"]>;
  private _consumerLeft: NonNullable<Stream.Options<VALUE, NAME>["consumerLeft"]>;

  private _$next?: Stream<Consumer<VALUE, `${NAME}Consumer`>, `${NAME}Next`>;
  private _$terminate?: Stream<"abort" | "complete", `${NAME}Terminate`>;
  private _$drain?: Stream<void, `${NAME}Drain`>;
  private _$consumerJoin?: Stream<Consumer<VALUE, `${NAME}Consumer`>, `${NAME}ConsumerJoin`>;
  private _$consumerLeft?: Stream<Consumer<VALUE, `${NAME}Consumer`>, `${NAME}ConsumerLeft`>;

  private _pulling: boolean;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    this._name = options?.name ?? ("$root" as NAME);
    this._consumers = new Map();
    this._queueFactory = options?.queueFactory;
    this._state = "active";
    this._pulling = false;

    this._next = options?.next ?? (() => {});
    this._drain = options?.drain ?? (() => {});
    this._terminate = options?.terminate ?? (() => {});
    this._consumerJoin = options?.consumerJoin ?? (() => {});
    this._consumerLeft = options?.consumerLeft ?? (() => {});

    this._scopeBinder = options?.scope ? new ScopeBinder(this, options.scope) : undefined;
  }
  get name() {
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
  get state() {
    return this._state;
  }
  get $next(): Stream<Consumer<VALUE, `${NAME}Consumer`>, `${NAME}Next`> {
    return (this._$next ??= new Stream({
      name: `${this._name}Next`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._$next = undefined;
      },
    }));
  }
  get $drain(): Stream<void, `${NAME}Drain`> {
    return (this._$drain ??= new Stream({
      name: `${this._name}Drain`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._$drain = undefined;
      },
    }));
  }
  get $terminate(): Stream<"abort" | "complete", `${NAME}Terminate`> {
    return (this._$terminate ??= new Stream({
      name: `${this._name}Terminate`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._$terminate = undefined;
      },
    }));
  }
  get $consumerJoin(): Stream<Consumer<VALUE, `${NAME}Consumer`>, `${NAME}ConsumerJoin`> {
    return (this._$consumerJoin ??= new Stream({
      name: `${this._name}ConsumerJoin`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._$consumerJoin = undefined;
      },
    }));
  }
  get $consumerLeft(): Stream<Consumer<VALUE, `${NAME}Consumer`>, `${NAME}ConsumerLeft`> {
    return (this._$consumerLeft ??= new Stream({
      name: `${this._name}ConsumerLeft`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._$consumerLeft = undefined;
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
    options?: Omit<Consumer.Options<VALUE, `${NAME}Consumer`>, "name">,
  ): Consumer<VALUE, `${NAME}Consumer`> {
    const { _next, _consumers, _queueFactory, _consumerLeft, _consumerJoin, _$next, _$consumerLeft, _$consumerJoin } =
      this;
    const { next, terminate, queue } = { ...options };

    if (_consumers.has(handler)) return _consumers.get(handler)!;

    const consumer = new Consumer(handler, {
      ...options,
      queue: queue ? queue : _queueFactory?.(),
      next: (self) => {
        next?.(self);
        if (this._pulling === false) {
          this._pulling = true;
          _next(this, self);
          _$next?.push(self);
        }
      },

      terminate: (self, reason) => {
        _consumers.delete(handler);
        this._optimizePush();

        terminate?.(self, reason);
        _consumerLeft(this, self);
        _$consumerLeft?.push(self);
        if (_consumers.size === 0 && this._state === "drain") this.terminate("complete");
      },
    });

    _consumers.set(handler, consumer);
    this._optimizePush();

    _consumerJoin(this, consumer);
    _$consumerJoin?.push(consumer);

    return consumer;
  }

  terminate(reason: "abort" | "complete"): void {
    this.push = () => {};
    if (reason === "abort") {
      this.terminate = () => {};
      this._state = "aborted";
    } else if (this._consumers.size) {
      this._state = "drain";
      this._drain(this);
      this._$drain?.push();
      return;
    } else {
      this.terminate = () => {};
      this._state = "completed";
    }

    for (const consumer of this._consumers.values()) {
      consumer.terminate(reason);
    }
    this._terminate(this, reason);
    this._$terminate?.push(reason);

    this._scopeBinder?.terminate(reason);
    this._scopeBinder = undefined;

    this._$terminate =
      this._$drain =
      this._$next =
      this._$consumerJoin =
      this._$consumerLeft =
      this._scopeBinder =
      this._queueFactory =
        undefined;
    this._next = this._drain = this._terminate = this._consumerJoin = this._consumerLeft = () => {};
  }

  pipe<CUSTOM_NAME extends NonEmptyString, OUTPUT extends Transformer<this, any, CUSTOM_NAME>>(
    transform: Transform<this, CUSTOM_NAME, OUTPUT>,
  ): OUTPUT;
  pipe<CUSTOM_NAME extends NonEmptyString, OUTPUT extends Transformer<this, any, CUSTOM_NAME>>(
    name: CUSTOM_NAME,
    transform: Transform<this, CUSTOM_NAME, OUTPUT>,
  ): OUTPUT;
  pipe<CUSTOM_NAME extends NonEmptyString, OUTPUT extends Transformer<this, any, CUSTOM_NAME>>(
    nameOrTransform: CUSTOM_NAME | Transform<this, CUSTOM_NAME, OUTPUT>,
    transform?: Transform<this, CUSTOM_NAME, OUTPUT>,
  ): OUTPUT {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }
}

export namespace Stream {
  export type State = "active" | "drain" | "aborted" | "completed";

  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    scope?: ScopeBinder.Scope;
    queueFactory?: QueueFactory<VALUE>;
    next?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE, `${NAME}Consumer`>) => void;
    drain?: (self: Stream<VALUE, NAME>) => void;
    terminate?: (self: Stream<VALUE, NAME>, reason: "abort" | "complete") => void;
    consumerJoin?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE, `${NAME}Consumer`>) => void;
    consumerLeft?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE, `${NAME}Consumer`>) => void;
  };
}
