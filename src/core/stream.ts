import { Consumer } from "./consumer";
import type { Closable, Evented, Named, Queue, Source } from "./types";
import type { Transformer } from "./transformer";
import { SourceLinker } from "./source-linker";
import { ScopeLinker } from "./scope-linker";
import { EventsLinker } from "./events-linker";

const NAME = "root";
export class Stream<VALUE, NAME extends string = Stream.Name>
  implements Source<VALUE, NAME>, Evented<EventsLinker.EventStreams<Stream.Events<VALUE>, NAME>>, Closable, Named<NAME>
{
  public readonly name: NAME;
  protected _consumers = new Map<Consumer.Handler<VALUE, NAME, any>, Consumer<VALUE, NAME, any>>();
  protected _state: Stream.State;
  protected _sourceLinker?: SourceLinker<VALUE>;
  protected _scopeLinker?: ScopeLinker;
  protected _eventsLinker: EventsLinker<Stream.Events<VALUE>, NAME, this>;
  protected _queueFactory?: Stream.QueueFactory<VALUE>;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    const { name, scope, source, queueFactory, events } = { ...options };

    this.name = name ?? (NAME as NAME);
    this._queueFactory = queueFactory;
    this._state = "active";
    this._eventsLinker = new EventsLinker(this, events);

    const { _eventsLinker } = this;

    if (source)
      this._sourceLinker = new SourceLinker(source, (_, value) => this.push(value), {
        events: {
          error: (_, error) => _eventsLinker.emit("error", error),
          abort: (_, error) => this.abort(error),
          complete: (_) => this.complete(),
        },
      });
    if (scope) {
      this._scopeLinker = new ScopeLinker(this, scope);
    }
  }
  protected _optimizePush(): void {
    const { _consumers } = this;
    switch (_consumers.size) {
      case 0:
        this.push = () => {};
        break;
      case 1:
        const consumer = _consumers.values().next().value!;
        this.push = (value) => consumer.push(value);
        break;
      default:
        this.push = (value) => {
          for (const consumer of _consumers.values()) {
            consumer.push(value);
          }
        };
    }
  }
  push(value: VALUE, hot = false): void {}

  listen<ERROR>(
    handler: Consumer.Handler<VALUE, NAME, ERROR>,
    options?: Consumer.Options<VALUE, NAME, ERROR>,
  ): Consumer<VALUE, NAME, ERROR> {
    const { _consumers, _sourceLinker, _eventsLinker } = this;

    if (_consumers.has(handler)) return _consumers.get(handler)!;

    const { events, queue, ...restOptions } = options ?? {};

    const { ready, abort, complete, error } = events ?? {};

    const consumer = new Consumer(handler, {
      ...restOptions,
      events: {
        ...events,
        ready: _sourceLinker
          ? ready
            ? (self) => {
                _sourceLinker!.next();
                ready(self);
              }
            : (self) => {
                _sourceLinker!.next();
              }
          : ready,
        abort: (consumer, error) => {
          _consumers.delete(handler);
          this._optimizePush();

          _eventsLinker.emit("consumerLeft", { reason: "abort", consumer, error });
          if (error) _eventsLinker.emit("error", { reason: "consumerAbort", error, consumer });

          if (_consumers.size === 0) {
            if (this._state === "drain") this._completed();
          }

          abort?.(consumer, error);
        },
        complete: (consumer) => {
          _consumers.delete(handler);
          this._optimizePush();

          _eventsLinker.emit("consumerLeft", { reason: "complete", consumer });

          if (_consumers.size === 0) if (this._state === "drain") this._completed();

          complete?.(consumer);
        },
        error: (consumer, err) => {
          _eventsLinker.emit("error", { reason: "consumerError", error: err, consumer });

          error?.(consumer, err);
        },
      },
      queue: queue ? queue : this._queueFactory?.(),
    });

    _consumers.set(handler, consumer);

    this._optimizePush();

    _eventsLinker.emit("consumerJoin", consumer);

    if (options?.isReady !== false && _sourceLinker) _sourceLinker.next();
    return consumer;
  }
  abort(error?: any): void {
    if (this._state === "aborted" || this._state === "completed") return;
    this._state = "aborted";
    this.push = () => {};
    this._optimizePush = () => {};

    for (const consumer of this._consumers.values()) {
      consumer.abort(error);
    }

    this._eventsLinker.emit("abort", error);
    if (error) this._eventsLinker.emit("error", { reason: "abort", error });

    this._clean("aborted", error);
  }
  complete(): void {
    if (this._state !== "active") return;
    if (this._consumers.size) {
      this._state = "drain";
      this.push = () => {};
      this._optimizePush = () => {};
      this._eventsLinker.emit("drain", undefined);
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

    this._eventsLinker.emit("complete", undefined);
    this._clean("completed");
  }
  protected _clean(reason: "aborted" | "completed", error?: any): void {
    if (reason === "aborted") {
      this._eventsLinker.abort(error);
      this._sourceLinker?.abort(error);
      this._scopeLinker?.abort(error);
    } else {
      this._eventsLinker.complete();
      this._sourceLinker?.complete();
      this._scopeLinker?.complete();
    }

    (this._eventsLinker as any) = this._queueFactory = this._sourceLinker = this._scopeLinker = undefined;
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
  get events(): EventsLinker.EventStreams<Stream.Events<VALUE>, NAME> {
    return this._eventsLinker.events;
  }
  get source(): Source<VALUE, any> | undefined {
    return this._sourceLinker?.source;
  }
  get scope(): ScopeLinker.Scope | undefined {
    return this._scopeLinker?.scope;
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnyStream = Stream<any, any>;

  export type Events<VALUE> = {
    drain: void;
    complete: void;
    abort: any;
    error: { error: any } & (
      | { reason: "abort"; consumer?: never }
      | { reason: "consumerError" | "consumerAbort"; consumer: Consumer<VALUE, any> }
    );
    consumerJoin: Consumer<VALUE, any>;
    consumerLeft: { consumer: Consumer<VALUE, any> } & (
      | { reason: "complete"; error?: never }
      | { reason: "abort"; error?: any }
    );
  };

  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE, NAME extends string> = {
    name?: NAME;
    source?: Source<VALUE, any>;
    scope?: ScopeLinker.Scope;
    queueFactory?: QueueFactory<VALUE>;
    events?: EventsLinker.EventsFunctions<Events<VALUE>, Stream<VALUE, NAME>>;
  };
  export type ExtractValue<T extends AnyStream | Transformer.AnyTransformer> =
    T extends Stream<infer VALUE, any>
      ? VALUE
      : Transformer.ExtractValue<T> extends never
        ? never
        : Transformer.ExtractValue<T>;

  export type ExtractName<T> = T extends { [k in "name"]: any } ? T["name"] : never;

  export type Transform<
    IN extends AnyStream,
    OUT_NAME extends string,
    OUT extends Transformer<IN, any, OUT_NAME> | IN,
  > = (inputStream: IN, name?: OUT_NAME) => OUT;

  export interface Streamable<VALUE, NAME extends string> {
    readonly stream: Stream<VALUE, NAME>;
  }
}
