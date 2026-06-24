import { Consumer } from "./consumer";
import type { Closable, Queue, Source } from "./types";
import type { Transformer, transformer } from "./transformer";
import { SourceLinker } from "./source-linker";
import { ScopeLinker } from "./scope-linker";
import { EventsLinker } from "./events-linker";

export class Stream<VALUE, NAME extends string = Stream.Name>
  implements Source<VALUE>, Closable<EventsLinker.EventsStream<Stream.Events<VALUE>, NAME>>
{
  public readonly name: NAME;
  protected _consumers = new Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>();
  protected _state: Stream.State;
  protected _sourceConsumer?: SourceLinker<VALUE>;
  protected _scopeBinder?: ScopeLinker;
  protected _queueFactory?: Stream.QueueFactory<VALUE>;
  protected _eventsProxy: EventsLinker<Stream.Events<VALUE>, NAME, this>;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    const { name, scope, source, queueFactory, ...hooks } = { ...options };

    this.name = name ?? (Stream.NAME as NAME);
    this._queueFactory = queueFactory;
    this._state = "active";
    this._eventsProxy = new EventsLinker(this.name, this, hooks);

    const { _eventsProxy } = this;
    if (source)
      this._sourceConsumer = new SourceLinker(source, (_, value) => this.push(value), {
        error: (_, error) => {
          _eventsProxy.emit("error", error);
        },
        abort: (_, error) => this.abort(error),
        complete: () => this.complete(),
      });
    if (scope) {
      this._scopeBinder = new ScopeLinker(this, scope);
    }
  }
  protected _optimizePush(): void {
    const { _eventsProxy } = this;
    switch (this._consumers.size) {
      case 0:
        this.push = () => {};
        break;
      case 1:
        const consumer = this._consumers.values().next().value!;

        this.push = _eventsProxy.has("push")
          ? (value) => {
              _eventsProxy.emit("push", value);
              consumer.push(value);
            }
          : (value) => {
              consumer.push(value);
            };
        break;
      default:
        this.push = _eventsProxy.has("push")
          ? (value) => {
              for (const consumer of this._consumers.values()) {
                _eventsProxy.emit("push", value);
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
    const { _consumers, _sourceConsumer, _eventsProxy } = this;
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

        _eventsProxy.emit("consumerLeft", consumer);

        if (_consumers.size === 0) {
          if (this._state === "drain") this._completed();
        }

        options?.abort?.(self, error);
      },
      complete: (self) => {
        _consumers.delete(handler);
        this._optimizePush();

        _eventsProxy.emit("consumerLeft", consumer);

        if (_consumers.size === 0) {
          if (this._state === "drain") this._completed();
        }

        options?.complete?.(self);
      },

      error: (self, error) => {
        console.log(error);

        _eventsProxy.emit("error", error);

        options?.error?.(self, error);
      },

      queue: options?.queue ? options.queue : this._queueFactory?.(),
    });

    _consumers.set(handler, consumer);

    this._optimizePush();

    _eventsProxy.emit("consumerJoin", consumer);

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

    this._eventsProxy.emit("abort", error);

    this._clean("aborted", error);
  }
  complete(): void {
    if (this._state !== "active") return;

    if (this._consumers.size) {
      this._state = "drain";
      this.push = () => {};
      this._eventsProxy.emit("drain", undefined);
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

    this._eventsProxy.emit("complete", undefined);
    this._clean("_completed");
  }
  protected _clean(reason: "aborted" | "_completed", error?: any): void {
    if (reason === "aborted") {
      for (const event of Object.values(this._eventsProxy ?? {})) event.abort(error);
      this._sourceConsumer?.abort(error);
      this._scopeBinder?.abort(error);
    } else {
      for (const event of Object.values(this._eventsProxy ?? {})) event.complete();
      this._sourceConsumer?.complete();
      this._scopeBinder?.complete();
    }

    (this._eventsProxy as any) = this._queueFactory = this._sourceConsumer = this._scopeBinder = undefined;
  }
  pipe<OUT_NAME extends string, OUT extends Transformer<this, any, OUT_NAME> | this>(
    transform: Stream.Transform<this, OUT_NAME, OUT>,
  ): OUT;
  pipe<OUT_NAME extends string, OUT extends Transformer<this, any, OUT_NAME> | this>(
    name: OUT_NAME,
    transform: Stream.Transform<this, OUT_NAME, OUT>,
  ): OUT;
  pipe<OUT_NAME extends string, OUT extends Transformer<this, any, OUT_NAME> | this>(
    nameOrTransform: OUT_NAME | Stream.Transform<this, OUT_NAME, OUT>,
    transform?: Stream.Transform<this, OUT_NAME, OUT>,
  ): OUT {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }
  get state(): Stream.State {
    return this._state;
  }
  get consumersCount(): number {
    return this._consumers.size;
  }
  get events(): EventsLinker.EventsStream<Stream.Events<VALUE>, NAME> {
    return this._eventsProxy.events;
  }
  get source(): Source<VALUE> | undefined {
    return this._sourceConsumer?.source;
  }
  get scope(): ScopeLinker.Scope | undefined {
    return this._scopeBinder?.scope;
  }
}

export function stream<VALUE, NAME extends string>(options?: Stream.Options<VALUE, NAME>): Stream<VALUE, NAME> {
  return new Stream(options);
}
export namespace Stream {
  export const NAME = "root";
  export type Name = typeof NAME;
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnyStream = Stream<any, any>;

  export type Events<VALUE> = {
    push: VALUE;
    drain: void;
    complete: void;
    abort: any;
    error: any;
    consumerJoin: Consumer<VALUE, any>;
    consumerLeft: Consumer<VALUE, any>;
  };
  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE, NAME extends string> = {
    name?: NAME;
    source?: Source<VALUE>;
    scope?: ScopeLinker.Scope;
    queueFactory?: QueueFactory<VALUE>;
  } & EventsLinker.EventsFunctions<Events<VALUE>, Stream<VALUE, NAME>>;
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
