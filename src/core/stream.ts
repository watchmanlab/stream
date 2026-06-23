import { Consumer } from "./consumer";
import type { Closable, Queue, Source } from "./types";
import type { Transformer, transformer } from "./transformer";
import { SourceConsumer } from "./source-consumer";
import { ScopeBinder } from "./scope-binder";

export class Stream<VALUE, NAME extends string = stream.Name>
  implements Source<VALUE>, Closable<stream.Events<VALUE, NAME>>
{
  public readonly name: NAME;
  protected _consumers = new Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>();
  protected _state: stream.State;
  protected _sourceConsumer?: SourceConsumer<VALUE>;
  protected _scopeBinder?: ScopeBinder;
  protected _queueFactory?: stream.QueueFactory<VALUE>;
  protected _events?: Partial<stream.Events<VALUE, NAME>>;
  protected _options?: stream.Options<VALUE, NAME>;
  constructor(options?: stream.Options<VALUE, NAME>) {
    this._options = { ...options };
    this.name = options?.name ?? (stream.NAME as NAME);
    this._queueFactory = options?.queueFactory;
    this._state = "active";

    if (options?.source)
      this._sourceConsumer = new SourceConsumer(options.source, (_, value) => this.push(value), {
        error: (_, error) => {
          options.error?.(this, error);
          this._events?.error?.push(error);
        },
        abort: (_, error) => this.abort(error),
        complete: () => this.complete(),
      });
    if (options?.scope) {
      this._scopeBinder = new ScopeBinder(this, options.scope);
    }
  }
  protected _optimizePush(): void {
    switch (this._consumers.size) {
      case 0:
        this.push = () => {};
        break;
      case 1:
        const consumer = this._consumers.values().next().value!;
        this.push = this._options?.push
          ? (value) => {
              this._options!.push!(this, value);
              consumer.push(value);
            }
          : (value) => {
              consumer.push(value);
            };
        break;
      default:
        this.push = this._options?.push
          ? (value) => {
              for (const consumer of this._consumers.values()) {
                this._options!.push!(this, value);
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
    options?: Consumer.Options<VALUE, ERROR>,
  ): Consumer<VALUE, ERROR> {
    const { _consumers, _sourceConsumer, _events, _options } = this;
    const { ready } = options ?? {};

    if (_consumers.has(handler)) return _consumers.get(handler)!;

    const consumer = new Consumer(handler, {
      ...options,
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
        _consumers.delete(handler);
        this._optimizePush();

        _options?.consumerLeft?.(this, consumer);
        _events?.consumerLeft?.push(consumer);

        if (_consumers.size === 0) {
          if (this._state === "drain") this._completed();
        }

        options?.abort?.(self, error);
      },
      complete: (self) => {
        _consumers.delete(handler);
        this._optimizePush();

        _options?.consumerLeft?.(this, consumer);
        _events?.consumerLeft?.push(consumer);

        if (_consumers.size === 0) {
          if (this._state === "drain") this._completed();
        }

        options?.complete?.(self);
      },

      error: (self, error) => {
        console.log(error);

        _options?.error?.(this, error);
        _events?.error?.push(error);
      },

      queue: options?.queue ? options.queue : this._queueFactory?.(),
    });

    _consumers.set(handler, consumer);
    this._optimizePush();

    _options?.consumerJoin?.(this, consumer);
    _events?.consumerJoin?.push(consumer);

    if (options?.isReady !== false && _sourceConsumer) {
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

    this._options?.abort?.(this, error);
    this._events?.abort?.push(error);

    this._clean("aborted", error);
  }
  complete(): void {
    if (this._state !== "active") return;

    if (this._consumers.size) {
      this._state = "drain";
      this.push = () => {};
      this._options?.drain?.(this);
      this._events?.drain?.push();
    } else {
      this._completed();
    }
    for (const consumer of this._consumers.values()) {
      consumer.complete();
    }
  }
  protected _completed(): void {
    this._state = "completed";
    this.push = () => {};

    this._options?.complete?.(this);
    this._events?.complete?.push();
    this._clean("_completed");
  }
  protected _clean(reason: "aborted" | "_completed", error?: any): void {
    if (reason === "aborted") {
      for (const event of Object.values(this._events ?? {})) event.abort(error);
      this._sourceConsumer?.abort(error);
      this._scopeBinder?.abort(error);
    } else {
      for (const event of Object.values(this._events ?? {})) event.complete();
      this._sourceConsumer?.complete();
      this._scopeBinder?.complete();
    }

    this._events = this._options = this._queueFactory = this._sourceConsumer = this._scopeBinder = undefined;
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
        const stream = new Stream({ name: this.name + p[0].toUpperCase() + p.slice(1) });
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

export function stream<VALUE, NAME extends string>(options?: stream.Options<VALUE, NAME>): Stream<VALUE, NAME> {
  return new Stream(options);
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
  export type Options<VALUE, NAME extends string> = {
    name?: NAME;
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
