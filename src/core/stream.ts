import { Consumer } from "./consumer";
import type { Prettify, Queue, Source } from "./types";
import type { Transformer, transformer } from "./transformer";
import { SourceConsumer } from "./source-consumer";
import { ScopeBinder } from "./scope-binder";

export class Stream<VALUE, NAME extends string = stream.Name> implements Source<VALUE> {
  protected _consumers = new Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>();
  protected _state: stream.State;
  protected _sourceConsumer?: SourceConsumer<VALUE>;
  protected _scopeBinder?: ScopeBinder;
  protected _queueFactory?: stream.QueueFactory<VALUE>;
  protected _events?: Partial<stream.Events<VALUE, NAME>>;
  constructor(
    public readonly name: NAME,
    private init?: stream.Init<VALUE, NAME>,
  ) {
    this._queueFactory = init?.queueFactory;
    this._state = "active";

    if (init?.source)
      this._sourceConsumer = new SourceConsumer(init.source, {
        handler: (_, value) => this.push(value),
        error: (_, error) => {
          init.error?.(this, error);
          this._events?.error?.push(error);
        },
        abort: (_, error) => this.abort(error),
        complete: () => this.complete(),
      });
    if (init?.scope) {
      this._scopeBinder = new ScopeBinder(this, init.scope);
    }
  }
  protected optimizePush(): void {
    switch (this._consumers.size) {
      case 0:
        this.push = () => {};
        break;
      case 1:
        const consumer = this._consumers.values().next().value!;
        this.push = this.init?.push
          ? (value) => {
              this.init!.push!(this, value);
              consumer.push(value);
            }
          : (value) => {
              consumer.push(value);
            };
        break;
      default:
        this.push = this.init?.push
          ? (value) => {
              for (const consumer of this._consumers.values()) {
                this.init!.push!(this, value);
                consumer.push(value);
              }
            }
          : (value) => {
              for (const consumer of this._consumers.values()) {
                consumer.push(value);
              }
            };
    }
  }
  push(value: VALUE, hot = false): void {}

  listen<ERROR>(
    handler: Consumer.Handler<VALUE, ERROR>,
    init?: Prettify<Omit<Consumer.Init<VALUE, ERROR>, "handler">>,
  ): Consumer<VALUE, ERROR>;
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Consumer<VALUE, ERROR>;
  listen<ERROR>(
    handlerOrInit: Consumer.Handler<VALUE, ERROR> | Consumer.Init<VALUE, ERROR>,
    _init?: Omit<Consumer.Init<VALUE, ERROR>, "handler">,
  ): Consumer<VALUE, ERROR> {
    const init = typeof handlerOrInit === "function" ? { handler: handlerOrInit, ..._init } : { ...handlerOrInit };

    const { _consumers, _sourceConsumer, _events, init: thisInit } = this;
    const { ready } = init;

    if (_consumers.has(init.handler)) return _consumers.get(init.handler)!;

    const consumer = new Consumer({
      ...init,
      ready: _sourceConsumer
        ? ready
          ? (self) => {
              _sourceConsumer!.next();
              ready(self);
            }
          : () => {
              _sourceConsumer!.next();
            }
        : ready,
      abort: (self, error) => {
        _consumers.delete(init.handler);
        this.optimizePush();

        thisInit?.consumerLeft?.(this, consumer);
        _events?.consumerLeft?.push(consumer);

        if (_consumers.size === 0) {
          if (this._state === "drain") this.completed();
        }

        init.abort?.(self, error);
      },
      complete: (self) => {
        _consumers.delete(init.handler);
        this.optimizePush();

        thisInit?.consumerLeft?.(this, consumer);
        _events?.consumerLeft?.push(consumer);

        if (_consumers.size === 0) {
          if (this._state === "drain") this.completed();
        }

        init.complete?.(self);
      },

      error: (self, error) => {
        console.log(error);

        thisInit?.error?.(this, error);
        _events?.error?.push(error);
      },

      queue: init.queue ? init.queue : this._queueFactory?.(),
    });

    _consumers.set(init.handler, consumer);
    this.optimizePush();

    thisInit?.consumerJoin?.(this, consumer);
    _events?.consumerJoin?.push(consumer);

    if (init.isReady !== false && _sourceConsumer) {
      _sourceConsumer.next();
    }

    return consumer;
  }
  abort(error?: any): void {
    if (this._state === "aborted" || this._state === "completed") return;
    this._state = "aborted";
    this.push = () => {};
    for (const consumer of this._consumers.values()) {
      consumer.abort(error);
    }

    this.init?.abort?.(this, error);
    this._events?.abort?.push(error);

    this.clean("aborted", error);
  }
  complete(): void {
    if (this._state !== "active") return;

    if (this._consumers.size) {
      this._state = "drain";
      this.push = () => {};
      this.init?.drain?.(this);
      this._events?.drain?.push();
    } else {
      this.completed();
    }
    for (const consumer of this._consumers.values()) {
      consumer.complete();
    }
  }
  protected completed(): void {
    this._state = "completed";
    this.push = () => {};

    this.init?.complete?.(this);
    this._events?.complete?.push();
    this.clean("completed");
  }
  protected clean(reason: "aborted" | "completed", error?: any): void {
    if (reason === "aborted") {
      for (const event of Object.values(this._events ?? {})) event.abort(error);
      this._sourceConsumer?.abort(error);
      this._scopeBinder?.abort(error);
    } else {
      for (const event of Object.values(this._events ?? {})) event.complete();
      this._sourceConsumer?.complete();
      this._scopeBinder?.complete();
    }

    this._events = this.init = this._queueFactory = this._sourceConsumer = this._scopeBinder = undefined;
  }
  pipe<OUT_NAME extends string, OUT extends Transformer<this, any, OUT_NAME> | this>(
    transform: stream.Transform<this, OUT_NAME, OUT>,
  ): OUT;
  pipe<OUT_NAME extends string, OUT extends Transformer<this, any, OUT_NAME> | this>(
    name: OUT_NAME,
    transform: stream.Transform<this, OUT_NAME, OUT>,
  ): OUT;
  pipe<OUT_NAME extends string, OUT extends Transformer<this, any, OUT_NAME> | this>(
    nameOrTransform: OUT_NAME | stream.Transform<this, OUT_NAME, OUT>,
    transform?: stream.Transform<this, OUT_NAME, OUT>,
  ): OUT {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }

  get state(): stream.State {
    return this._state;
  }
  get consumersCount(): number {
    return this._consumers.size;
  }
  get events(): stream.Events<VALUE, NAME> {
    if (!this._events) this._events = {};

    return new Proxy(this._events as stream.Events<VALUE, NAME>, {
      get: (target, p: string, receiver) => {
        if (p in target) return Reflect.get(target, p, receiver);
        const stream = new Stream(this.name + p[0].toUpperCase() + p.slice(1));
        (this._events as any)[p] = stream;
        return stream;
      },
    });
  }
  get source(): Source<VALUE> | undefined {
    return this._sourceConsumer?.source;
  }
  get scope(): ScopeBinder.Scope | undefined {
    return this._scopeBinder?.scope;
  }
}
export function stream<VALUE, NAME extends string>(name: NAME, init?: stream.Init<VALUE, NAME>): Stream<VALUE, NAME>;
export function stream<VALUE, NAME extends string>(init?: stream.Init<VALUE, NAME>): Stream<VALUE, NAME>;
export function stream<VALUE, NAME extends string>(
  nameOrInit: NAME | stream.Init<VALUE, NAME>,
  init?: stream.Init<VALUE, NAME>,
): Stream<VALUE, NAME> {
  return typeof nameOrInit === "string" ? new Stream(nameOrInit, init) : new Stream(stream.NAME as NAME, init);
}
export namespace stream {
  export const NAME = "root";
  export type Name = typeof NAME;
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnyStream = Stream<any, any>;

  export type Events<VALUE, NAME extends string> = {
    drain: Stream<void, `${NAME}Drain`>;
    complete: Stream<void, `${NAME}Complete`>;
    abort: Stream<any, `${NAME}Abort`>;
    error: Stream<any, `${NAME}Error`>;
    consumerJoin: Stream<Consumer<VALUE, any>, `${NAME}ConsumerJoin`>;
    consumerLeft: Stream<Consumer<VALUE, any>, `${NAME}ConsumerLeft`>;
  };
  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Init<VALUE, NAME extends string> = {
    source?: Source<VALUE>;
    scope?: ScopeBinder.Scope;
    queueFactory?: QueueFactory<VALUE>;
    push?: (self: Stream<VALUE, NAME>, value: VALUE) => void;
    drain?: (self: Stream<VALUE, NAME>) => void;
    complete?: (self: Stream<VALUE, NAME>) => void;
    abort?: (self: Stream<VALUE, NAME>, error?: any) => void;
    error?: (self: Stream<VALUE, NAME>, error: any) => void;
    consumerJoin?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE, any>) => void;
    consumerLeft?: (self: Stream<VALUE, NAME>, consumer: Consumer<VALUE, any>) => void;
  };
  export type ExtractValue<T extends AnyStream | transformer.AnyTransformer> =
    T extends Stream<infer VALUE, any>
      ? VALUE
      : transformer.ExtractValue<T> extends never
        ? never
        : transformer.ExtractValue<T>;

  export type ExtractName<T> = T extends { [k in "name"]: any } ? T["name"] : never;

  export type Transform<
    IN extends AnyStream,
    OUT_NAME extends string,
    OUT extends Transformer<IN, any, OUT_NAME> | IN,
  > = (inputStream: IN, name?: OUT_NAME) => OUT;
}
