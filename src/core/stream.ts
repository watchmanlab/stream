import { Consumer } from "./consumer";
import type { Prettify, Queue, Source, EventShape } from "./types";
import type { Transformer } from "./transformer";

export class Stream<VALUE, NAME extends string = Stream.Name> implements Source<VALUE> {
  private _consumers = new Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>();
  private _state: Stream.State;
  readonly name: NAME;
  private _source?: Source<VALUE>;
  private _sourceConsumer?: Consumer<VALUE, any>;
  private _scope?: Stream.Scope;
  private _scopeConsumers?: Consumer<any, any>[];

  private _queue?: Stream.QueueFactory<VALUE>;
  private _event?: Stream<Stream.Event<VALUE>, `${NAME}Event`>;
  private _pulling = false;

  constructor(init?: Stream.Init<VALUE, NAME>) {
    this.name = init?.name ?? (Stream.NAME as NAME);
    this._source = init?.source;
    this._scope = init?.scope;
    this._queue = init?.queue;
    this._state = "active";

    this.bindScope();
  }

  private bindSource() {
    if (!this._source) return;

    this._sourceConsumer = this._source.listen({
      handler: (self, value) => {
        this._pulling = false;
        this.push(value);
      },
      error: (self, error) => {
        this._event?.push({ type: "error", error });
      },
      isReady: false,
    });
  }
  private unbindSource() {
    this._sourceConsumer?.complete();
    this._sourceConsumer = undefined;
  }
  private bindScope() {
    if (!this._scope) return;
    this._scopeConsumers = [];
    if (this._scope instanceof Stream) {
      const scopeConsumer = this._scope.event.listen((self, e) => {
        switch (e.type) {
          case "abort":
            this.abort(e.error);
            break;
          case "complete":
            this.complete();
            break;
        }
        self.next();
      });
      this._scopeConsumers.push(scopeConsumer);
    } else if (this._scope.any) {
      const scopes = new Set(this._scope.any);

      scopes.forEach((scope) =>
        this._scopeConsumers!.push(
          scope.event.listen((self, e) => {
            switch (e.type) {
              case "abort":
                this.abort(e.error);
                scopes.clear();
                break;
              case "complete":
                this.complete();
                scopes.clear();
                break;
            }
            self.next();
          }),
        ),
      );
    } else {
      const scopes = new Set(this._scope.all);
      let count = scopes.size;

      scopes.forEach((scope) => {
        this._scopeConsumers!.push(
          scope.event.listen((self, e) => {
            switch (e.type) {
              case "abort":
                this.abort(e.error);
                scopes.clear();
                break;
              case "complete":
                if (!--count) {
                  this.complete();
                  scopes.clear();
                }
                break;
            }
            self.next();
          }),
        );
      });
    }
  }

  push(value: VALUE): void {
    for (const consumer of this._consumers.values()) {
      consumer.push(value);
    }
  }
  listen<ERROR>(
    handler: Consumer.Handler<VALUE, ERROR>,
    init?: Prettify<Omit<Consumer.Init<VALUE, ERROR>, "handler" | "name">>,
  ): Consumer<VALUE, ERROR>;
  listen<ERROR>(init: Prettify<Omit<Consumer.Init<VALUE, ERROR>, "name">>): Consumer<VALUE, ERROR>;
  listen<ERROR>(
    handlerOrInit: Consumer.Handler<VALUE, ERROR> | Prettify<Omit<Consumer.Init<VALUE, ERROR>, "name">>,
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
          this.unbindSource();
          if (this._state === "drain") this.completed();
        }

        init.abort?.(self, error);
      },
      complete: (self) => {
        this._consumers.delete(init.handler);

        this._event?.push({ type: "consumer-left", consumer });

        if (this._consumers.size === 0) {
          this.unbindSource();
          if (this._state === "drain") this.completed();
        }

        init.complete?.(self);
      },
      ready: (self) => {
        if (!this._pulling && this._sourceConsumer) {
          this._pulling = true;
          this._sourceConsumer.next();
        }
        init.ready?.(self);
      },
      error: (self, error) => {
        console.log(error);
        this._event?.push({ type: "error", error: error });
      },

      queue: init.queue ? init.queue : this._queue?.(),
    });

    this._consumers.set(init.handler, consumer);

    if (this._consumers.size === 1) this.bindSource();

    this._event?.push({ type: "consumer-join", consumer });

    if (init.isReady !== false && !this._pulling && this._sourceConsumer) {
      this._pulling = true;
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
  private completed(): void {
    this._state = "completed";
    this.push = () => {};
    this._event?.push({ type: "complete" });
    this.clean("completed");
  }
  private clean(reason: "aborted" | "completed", error?: any): void {
    if (reason === "aborted") {
      this._event?.abort();
      this._sourceConsumer?.abort(error);
      this._scopeConsumers?.forEach((c) => c.abort());
    } else {
      this._event?.complete();
      this._sourceConsumer?.complete();
      this._scopeConsumers?.forEach((c) => c.complete());
    }

    this._event = this._queue = this._sourceConsumer = this._scopeConsumers = this._scope = this._source = undefined;
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
    return this._source;
  }
  static fromIterable<VALUE>(iterable: Iterable<VALUE>) {
    //TODO
  }
}

export namespace Stream {
  export const NAME = "root";
  export type Name = typeof NAME;
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnySmoker = Stream<any, any>;
  export type Scope =
    | AnySmoker
    | { any: [AnySmoker, ...AnySmoker[]]; all?: never }
    | { any?: never; all: [AnySmoker, ...AnySmoker[]] };

  export type Event<VALUE> =
    | EventShape<"drain" | "complete">
    | EventShape<"abort" | "error", { error: any }>
    | EventShape<"consumer-join" | "consumer-left", { consumer: Consumer<VALUE, any> }>;

  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Init<VALUE, NAME extends string> = {
    name?: NAME;
    source?: Source<VALUE>;
    scope?: Scope;
    queue?: QueueFactory<VALUE>;
  };
  export type ExtractValue<T extends AnySmoker | Transformer.AnyTransformer> =
    T extends Stream<infer VALUE, any>
      ? VALUE
      : Transformer.ExtractValue<T> extends never
        ? never
        : Transformer.ExtractValue<T>;

  export type ExtractName<T> = T extends { [k in "name"]: any } ? T["name"] : never;

  export type Transform<
    IN extends AnySmoker,
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
    //     consumer.next();
    //   });
    //   return;
    // }

    self.next();
  });
  // stream.listen((self, value) => {
  //   if (value === MAX) console.log("foo", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");

  //   self.next();
  // });
  // stream.listen((self, value) => {
  //   if (value === MAX) console.log("bar", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");

  //   self.next();
  // });
  // stream.listen((self, value) => {
  //   if (value === MAX) console.log("baz", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");

  //   self.next();
  // });

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
}

// bench(); //foo 10 000 000 275 ms

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
