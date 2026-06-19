import { Consumer } from "./consumer";
import type { Prettify, Queue, Source, EventShape, AnyEventShape } from "./types";
import type { Transformer } from "./transformer";
import { SourceConsumer } from "./source-consumer";
import { ScopeBinder } from "./scope-binder";

export class Stream<VALUE, NAME extends string = Stream.Name> implements Source<VALUE> {
  protected _consumers = new Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>();
  protected _state: Stream.State;
  readonly name: NAME;
  protected _sourceConsumer?: SourceConsumer<VALUE>;
  protected _scopeBinder?: ScopeBinder;
  protected _queue?: Stream.QueueFactory<VALUE>;
  protected _event?: Stream<Stream.Event<VALUE>, `${NAME}Event`>;

  constructor(init?: Stream.Init<VALUE, NAME>) {
    this.name = init?.name ?? (Stream.NAME as NAME);
    this._queue = init?.queue;
    this._state = "active";

    if (init?.source)
      this._sourceConsumer = new SourceConsumer(init.source, {
        handler: (self, value) => {
          this.push(value);
        },
        error: (self, error) => {
          this._event?.push({ type: "error", error });
        },
      });
    if (init?.scope) {
      this._scopeBinder = new ScopeBinder(this, init.scope);
    }
  }
  push(value: VALUE): void {
    for (const consumer of this._consumers.values()) {
      consumer.push(value);
    }
  }
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
      abort: (self, error) => {
        this._consumers.delete(init.handler);

        this._event?.push({ type: "consumer-left", consumer });

        if (this._consumers.size === 0) {
          if (this._state === "drain") this.completed();
        }

        init.abort?.(self, error);
      },
      complete: (self) => {
        this._consumers.delete(init.handler);

        this._event?.push({ type: "consumer-left", consumer });

        if (this._consumers.size === 0) {
          if (this._state === "drain") this.completed();
        }

        init.complete?.(self);
      },
      ready: (self) => {
        this._sourceConsumer?.next();
        init.ready?.(self);
      },
      error: (self, error) => {
        console.log(error);
        this._event?.push({ type: "error", error: error });
      },

      queue: init.queue ? init.queue : this._queue?.(),
    });

    this._consumers.set(init.handler, consumer);

    this._event?.push({ type: "consumer-join", consumer });

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
    this._event?.push({ type: "abort", error });

    this.clean("aborted", error);
  }
  complete(): void {
    if (this._state !== "active") return;

    if (this._consumers.size) {
      this._state = "drain";
      this.push = () => {};
      this._event?.push({ type: "drain" });
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
    this._event?.push({ type: "complete" });
    this.clean("completed");
  }
  protected clean(reason: "aborted" | "completed", error?: any): void {
    if (reason === "aborted") {
      this._event?.abort();
      this._sourceConsumer?.abort(error);
      this._scopeBinder?.abort(error);
    } else {
      this._event?.complete();
      this._sourceConsumer?.complete();
      this._scopeBinder?.complete();
    }

    this._event = this._queue = this._sourceConsumer = this._scopeBinder = undefined;
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
  get event(): Stream<Stream.Event<VALUE>, `${NAME}Event`> {
    if (!this._event) this._event = new Stream({ name: `${this.name}Event` });
    return new Stream({ name: this._event.name, source: this._event });
  }
  get source(): Source<VALUE> | undefined {
    return this._sourceConsumer?.source;
  }
  get scope(): ScopeBinder.Scope | undefined {
    return this._scopeBinder?.scope;
  }
  static fromIterable<VALUE>(iterable: Iterable<VALUE>) {
    //TODO
  }
}

export namespace Stream {
  export const NAME = "root";
  export type Name = typeof NAME;
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnyStream = Stream<any, any>;

  export type Event<VALUE> =
    | EventShape<"drain" | "complete">
    | EventShape<"abort" | "error", { error: any }>
    | EventShape<"consumer-join" | "consumer-left", { consumer: Consumer<VALUE, any> }>;

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
  const MAX = 10_000_000;
  const stream = new Stream<number>();

  const start = performance.now();
  stream.listen((self, value) => {
    if (value === MAX) console.log("moo", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");

    // if (value === 1000) {
    //   queueMicrotask(() => {
    //     console.log("promise resolved", value);
    //     self.next();
    //   });
    //   return;
    // }

    self.next();
  });

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
}

bench(); //moo 10 000 000 727 ms

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
  smoker.event.listen((self, e) => {
    if (e.type === "error") {
      console.log("error caugh:", e.error);
    }
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
