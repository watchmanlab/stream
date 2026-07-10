import { Consumer } from "./consumer";
import type {
  Closable,
  Evented,
  EventHandlers,
  EventStreams,
  Named,
  NonEmptyString,
  CloseEvents,
  Queue,
  Source,
  State,
  Transform,
} from "./types";
import type { Transformer } from "./transformer";
import { SourceLinker } from "./source-linker";
import { ScopeLinker } from "./scope-linker";
import { EventsLinker } from "./events-linker";
import { InfosLinker } from "./infos-linker";

const NAME = "root";
export class Stream<VALUE, NAME extends NonEmptyString = Stream.Name>
  implements Source<VALUE>, Evented<Stream.Events<VALUE>, NAME>, Closable, Named<NAME>
{
  public readonly name: NAME;
  protected _consumers = new Map<Consumer.Handler<VALUE, any, any>, Consumer<VALUE, any, any>>();
  protected _state: State;
  protected _sourceLinker?: SourceLinker<VALUE>;
  protected _scopeLinker?: ScopeLinker;
  protected _eventsLinker: EventsLinker<Stream.Events<VALUE>, NAME, this>;
  protected _infosLinker: InfosLinker<Stream.Infos>;
  protected _queueFactory?: Stream.QueueFactory<VALUE>;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    const { name, scope, source, queueFactory, events } = { ...options };

    this.name = name ?? (NAME as NAME);
    this._queueFactory = queueFactory;
    this._state = "active";
    this._eventsLinker = new EventsLinker(this, events);
    this._infosLinker = new InfosLinker({
      state: () => this._state,
      consumersCount: () => this._consumers.size,
    });

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
  push(value: VALUE): void {}

  listen<ERROR, CUSTOM_NAME extends NonEmptyString = `${NAME}Consumer`>(
    handler: Consumer.Handler<VALUE, ERROR, CUSTOM_NAME>,
    options?: Consumer.Options<VALUE, ERROR, CUSTOM_NAME>,
  ): Consumer<VALUE, ERROR, CUSTOM_NAME> {
    const { _consumers, _sourceLinker, _eventsLinker, name } = this;

    if (_consumers.has(handler)) return _consumers.get(handler)!;

    const { events, queue, name: customName, ...restOptions } = options ?? {};

    const { next, abort, complete, error } = events ?? {};

    const consumer = new Consumer(handler, {
      ...restOptions,
      name: customName ?? (`${name}Consumer` as CUSTOM_NAME),
      queue: queue ? queue : this._queueFactory?.(),
      events: {
        ...events,
        next: _sourceLinker
          ? next
            ? (self, error) => (_sourceLinker!.next(error), next(self, error))
            : (_, error) => _sourceLinker!.next(error)
          : next,
        abort: (consumer, error) => {
          _consumers.delete(handler);
          this._optimizePush();

          _eventsLinker.emit("consumerLeft", consumer);
          if (error) _eventsLinker.emit("error", error);

          if (_consumers.size === 0 && this._state === "drain") this._completed();

          abort?.(consumer, error);
        },
        complete: (consumer) => {
          _consumers.delete(handler);
          this._optimizePush();

          _eventsLinker.emit("consumerLeft", consumer);

          if (_consumers.size === 0 && this._state === "drain") this._completed();

          complete?.(consumer);
        },
        error: (consumer, err) => {
          _eventsLinker.emit("error", err);

          error?.(consumer, err);
        },
      },
    });

    _consumers.set(handler, consumer);

    this._optimizePush();

    _eventsLinker.emit("consumerJoin", consumer, (error) => _eventsLinker.emit("error", error));

    if (options?.isReady !== false && _sourceLinker) _sourceLinker.next();
    return consumer;
  }
  abort(error?: any): void {
    this.push = this.abort = this.complete = this._optimizePush = () => {};

    this._state = "aborted";

    for (const consumer of this._consumers.values()) {
      consumer.abort(error);
    }

    if (error) this._eventsLinker.emit("error", error);

    this._eventsLinker.emit("abort", error, (error) => this._eventsLinker.emit("error", error));

    this._clean("aborted", error);
  }
  complete(): void {
    this.push = this.complete = this._optimizePush = () => {};
    try {
      if (this._consumers.size) {
        this._state = "drain";
        this._eventsLinker.emit("drain", undefined);
      } else {
        this._completed();
      }
    } catch (error) {
      this._eventsLinker.emit("error", error);
    }
    for (const consumer of this._consumers.values()) {
      consumer.complete();
    }
  }
  protected _completed(): void {
    this.push = this.abort = this.complete = this._optimizePush = () => {};

    this._state = "completed";

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
  }
  pipe<OUT_NAME extends NonEmptyString, OUT extends Transformer<this, any, OUT_NAME> | this>(
    transform: Transform<this, OUT_NAME, OUT>,
  ): OUT;
  pipe<OUT_NAME extends NonEmptyString, OUT extends Transformer<this, any, OUT_NAME> | this>(
    name: OUT_NAME,
    transform: Transform<this, OUT_NAME, OUT>,
  ): OUT;
  pipe<OUT_NAME extends NonEmptyString, OUT extends Transformer<this, any, OUT_NAME> | this>(
    nameOrTransform: OUT_NAME | Transform<this, OUT_NAME, OUT>,
    transform?: Transform<this, OUT_NAME, OUT>,
  ): OUT {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }
  get infos(): Stream.Infos {
    return this._infosLinker.infos;
  }
  get events(): EventStreams<Stream.Events<VALUE>, NAME> {
    return this._eventsLinker.events;
  }
}

export namespace Stream {
  export type Name = typeof NAME;

  export type Events<VALUE> = {
    drain: void;
    complete: void;
    abort: any;
    error: any;
    consumerJoin: Consumer<VALUE, any, any>;
    consumerLeft: Consumer<VALUE, any, any>;
  };
  export type Infos = {
    state: State;
    consumersCount: number;
  };

  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    source?: Source<VALUE>;
    scope?: ScopeLinker.Scope;
    queueFactory?: QueueFactory<VALUE>;
    events?: EventHandlers<Events<VALUE>, Stream<VALUE, NAME>>;
  };
}
