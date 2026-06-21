import { Consumer } from "./consumer";
import type { Prettify, Queue, Source } from "./types";
import type { Transformer } from "./transformer";
import { SourceConsumer } from "./source-consumer";
import { ScopeBinder } from "./scope-binder";
import { map } from "../transformers/map";

export class Stream<VALUE, NAME extends string = Stream.Name> implements Source<VALUE> {
  readonly name: NAME;
  protected _consumers = new Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>();
  protected _state: Stream.State;
  protected _sourceConsumer?: SourceConsumer<VALUE>;
  protected _scopeBinder?: ScopeBinder;
  protected _queue?: Stream.QueueFactory<VALUE>;
  protected _events?: Partial<Stream.Events<VALUE, NAME>>;
  constructor(init?: Stream.Init<VALUE, NAME>) {
    this.name = init?.name ?? (Stream.NAME as NAME);
    this._queue = init?.queue;
    this._state = "active";

    if (init?.source)
      this._sourceConsumer = new SourceConsumer(init.source, {
        handler: (_, value) => this.push(value),
        error: (_, error) => this._events?.error?.push(error),
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
        this.push = (value: VALUE) => consumer.push(value);
        break;
      default:
        this.push = (value: VALUE) => {
          for (const consumer of this._consumers.values()) {
            consumer.push(value);
          }
        };
    }
  }
  push(value: VALUE): void {}
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

    if (this._consumers.has(init.handler)) return this._consumers.get(init.handler)!;

    const consumer = new Consumer({
      ...init,
      ready: this._sourceConsumer
        ? init.ready
          ? (self) => {
              this._sourceConsumer!.next();
              init.ready!(self);
            }
          : (_) => {
              this._sourceConsumer!.next();
            }
        : init.ready,
      abort: (self, error) => {
        this._consumers.delete(init.handler);
        this.optimizePush();

        this._events?.consumerLeft?.push(consumer);

        if (this._consumers.size === 0) {
          if (this._state === "drain") this.completed();
        }

        init.abort?.(self, error);
      },
      complete: (self) => {
        this._consumers.delete(init.handler);
        this.optimizePush();

        this._events?.consumerLeft?.push(consumer);

        if (this._consumers.size === 0) {
          if (this._state === "drain") this.completed();
        }

        init.complete?.(self);
      },

      error: (self, error) => {
        console.log(error);
        this._events?.error?.push(error);
      },

      queue: init.queue ? init.queue : this._queue?.(),
    });

    this._consumers.set(init.handler, consumer);
    this.optimizePush();

    this._events?.consumerJoin?.push(consumer);

    if (init.isReady !== false && this._sourceConsumer) {
      this._sourceConsumer.next();
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

    this._events?.abort?.push(error);

    this.clean("aborted", error);
  }
  complete(): void {
    if (this._state !== "active") return;

    if (this._consumers.size) {
      this._state = "drain";
      this.push = () => {};
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

    this._events = this._queue = this._sourceConsumer = this._scopeBinder = undefined;
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
  compose() {}
  get state(): Stream.State {
    return this._state;
  }
  get consumersCount(): number {
    return this._consumers.size;
  }
  get events(): Stream.Events<VALUE, NAME> {
    if (!this._events) this._events = {};

    return new Proxy(this._events as Stream.Events<VALUE, NAME>, {
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

export namespace Stream {
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
    name?: NAME;
    source?: Source<VALUE>;
    scope?: ScopeBinder.Scope;
    queue?: QueueFactory<VALUE>;
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
}

function bench() {
  const MAX = 70_000_000;
  const stream = new Stream<number>();

  const start = performance.now();
  stream.listen((self, value) => {
    if (value === MAX) console.log("moo", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");

    if (value === 1000) {
      value++;
      // queueMicrotask(() => {
      //   console.log("promise resolved", value);
      //   self.next();
      // });
      // return;
    }

    self.next();
  });

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
}

// bench(); //moo 70 000 000 997 ms

function sequential() {
  const smoker = new Stream<number>();

  smoker.listen(async (self, value) => {
    await new Promise((r) => setTimeout(r, Math.random() * 1000));
    console.log("sequential", value);
    self.next();
  });

  smoker.push(1);
  smoker.push(2);
  smoker.push(3);
}

// sequential();
function concurrent() {
  const smoker = new Stream<number>();

  smoker.listen(async (self, value) => {
    self.next();
    await new Promise((r) => setTimeout(r, Math.random() * 1000));
    console.log("concurrent", value);
  });

  // consumer.next();

  smoker.push(1);
  smoker.push(2);
  smoker.push(3);
}

// concurrent();

function errorHandling() {
  const smoker = new Stream<number>();
  smoker.events.error.listen((self, e) => {
    console.log("error caugh:", e.error);

    self.next();
  });

  smoker.listen((self, value) => {
    if (value === 3) throw "kechmahaja";
    console.log(value);

    self.next();
  });

  smoker.push(1);
  smoker.push(2);
  smoker.push(3);
}

// errorHandling();
