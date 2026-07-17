import { Consumer } from "./consumer";
import type {
  Closable,
  Evented,
  EventHandlers,
  EventStreams,
  Named,
  NonEmptyString,
  Queue,
  Source,
  State,
  Transform,
} from "./types";
import type { Transformer } from "./transformer";

import { ScopeLinker } from "./scope-linker";
import { EventsLinker } from "./events-linker";
import { SourceLinker } from "./source-linker";
import { InfosLinker } from "./infos-linker";

export class Stream<VALUE, NAME extends NonEmptyString = "root">
  implements Source<VALUE>, Evented<Stream.Events<VALUE>, NAME>, Closable, Named<NAME>
{
  public readonly name: NAME;
  protected _consumers = new Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>();
  protected _state: State;
  protected _sourceLinker?: SourceLinker<VALUE>;
  protected _scopeLinker?: ScopeLinker;
  protected _eventsLinker: EventsLinker<Stream.Events<VALUE>, NAME, this>;
  protected _infosLinker: InfosLinker<Stream.Infos>;
  protected _queueFactory?: Stream.QueueFactory<VALUE>;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    const { name, scope, source, queueFactory, events } = { ...options };

    this.name = name ?? ("root" as NAME);
    this._queueFactory = queueFactory;
    this._state = "active";
    this._eventsLinker = new EventsLinker(this, events);
    this._infosLinker = new InfosLinker({
      state: () => this._state,
      consumersCount: () => this._consumers.size,
    });

    if (source)
      this._sourceLinker = new SourceLinker(source, (_, value) => this.push(value), {
        events: {
          abort: (_) => this.abort(),
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

  listen<CUSTOM_NAME extends NonEmptyString = `${NAME}Consumer`>(
    handler: Consumer.Handler<VALUE, CUSTOM_NAME>,
    options?: Consumer.Options<VALUE, CUSTOM_NAME>,
  ): Consumer<VALUE, CUSTOM_NAME> {
    const { _consumers, _sourceLinker, _eventsLinker, name } = this;

    if (_consumers.has(handler)) return _consumers.get(handler)!;

    const { events, queue, name: customName, ...restOptions } = options ?? {};

    const { next, abort, complete } = events ?? {};

    const consumer = new Consumer(handler, {
      ...restOptions,
      name: customName ?? (`${name}Consumer` as CUSTOM_NAME),
      queue: queue ? queue : this._queueFactory?.(),
      events: {
        ...events,
        next: _sourceLinker
          ? next
            ? (self) => (_sourceLinker!.next(), next(self))
            : () => _sourceLinker!.next()
          : next,
        abort: (consumer) => {
          _consumers.delete(handler);
          this._optimizePush();

          _eventsLinker.emit("consumerLeft", consumer);

          if (_consumers.size === 0 && this._state === "drain") this._completed();

          abort?.(consumer);
        },
        complete: (consumer) => {
          _consumers.delete(handler);
          this._optimizePush();

          _eventsLinker.emit("consumerLeft", consumer);

          if (_consumers.size === 0 && this._state === "drain") this._completed();

          complete?.(consumer);
        },
      },
    });

    _consumers.set(handler, consumer);

    this._optimizePush();

    _eventsLinker.emit("consumerJoin", consumer);

    if (options?.ready !== false && _sourceLinker) _sourceLinker.next();
    return consumer;
  }
  bindScope(scope: ScopeLinker.Scope): () => void {
    const linker = new ScopeLinker(this, scope);

    return () => {
      linker.abort();
    };
  }
  abort(): void {
    this.push = this.abort = this.complete = this._optimizePush = () => {};

    this._state = "aborted";

    for (const consumer of this._consumers.values()) {
      consumer.abort();
    }

    this._eventsLinker.emit("abort", undefined);

    this._clean("aborted");
  }
  complete(): void {
    this.push = this.complete = this._optimizePush = () => {};
    if (this._consumers.size) {
      this._state = "drain";
      this._eventsLinker.emit("drain", undefined);
    } else {
      this._completed();
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
  protected _clean(reason: "aborted" | "completed"): void {
    if (reason === "aborted") {
      this._eventsLinker.abort();
      this._sourceLinker?.abort();
      this._scopeLinker?.abort();
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
  export type State = "active" | "draining" | "aborted" | "completed";
  export type Events<VALUE> = {
    drain: void;
    complete: void;
    abort: any;
    consumerJoin: Consumer<VALUE, any>;
    consumerLeft: Consumer<VALUE, any>;
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
  };
}
