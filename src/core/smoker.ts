import { Consumer } from "./consumer";
import type { Prettify, Queue, Source, EventShape } from "./types";
import type { Transformer } from "./transformer";

export class Smoker<VALUE, NAME extends string = Smoker.Name> {
  private _consumers = new Map<Consumer.Handler<VALUE, any, NAME>, Consumer<VALUE, any, NAME>>();
  private _state: Smoker.State;
  readonly name: NAME;
  private _sourceConsumer?: Consumer<VALUE, any, any>;
  private _fireEvent: Smoker.OnEvent<VALUE, NAME>;
  private _queueFactory?: Smoker.QueueFactory<VALUE>;
  private _event?: Smoker<Smoker.Event<VALUE, NAME>, `${NAME}Event`>;
  private _pulling = false;
  constructor(init?: Smoker.Init<VALUE, NAME>) {
    this.name = init?.name ?? (Smoker.NAME as NAME);
    this._fireEvent = (event: Smoker.Event<VALUE, NAME>) => {
      init?.onEvent?.(event);
      this._event?.push(event);
    };
    this._queueFactory = init?.queueFactory;

    this._state = "active";

    if (init?.source) {
      if (init.source instanceof Smoker) {
        this._sourceConsumer = init.source.listen({
          handler: (value) => {
            this._pulling = false;
            this.push(value);
          },
          onEvent: (event) => {
            switch (event.type) {
              case "abort":
                this.abort(event.error);
                break;
              case "complete":
                this.complete();
            }
          },
          isReady: false,
        });
      } else {
        this._sourceConsumer = init?.source;
        this._sourceConsumer.get("event").listen((event, self) => {
          switch (event.type) {
            case "ready":
              this._pulling = false;
              break;
            case "abort":
              this.abort(event.error);
              break;
            case "complete":
              this.complete();
          }
          self.next();
        });
      }
    }
    if (init?.scope) {
      if (init.scope instanceof Smoker) {
        init.scope.get("event").listen((e) => {
          switch (e.type) {
            case "abort":
              this.abort(e.error);
              break;
            case "complete":
              this.complete();
              break;
          }
        });
      } else if (init.scope.any) {
        const others: Consumer.AnyConsumer[] = [];
        new Set(init.scope.any).forEach((other) =>
          others.push(
            other.get("event").listen((e) => {
              switch (e.type) {
                case "abort":
                  this.abort(e.error);
                  break;
                case "complete":
                  this.complete();
                  break;
              }

              others.length = 0;
            }),
          ),
        );
      } else {
        const others = new Set(init.scope.all);
        let count = others.size;
        others.forEach((other) => {
          other.get("event").listen((e) => {
            switch (e.type) {
              case "abort":
                this.abort(e.error);
                others.clear();
                break;
              case "complete":
                if (!count--) {
                  this.complete();
                  others.clear();
                }
                break;
            }
          });
        });
      }
    }
  }
  protected ready(value: VALUE) {
    this.push(value);
    this._pulling = false;
  }

  push(value: VALUE): void {
    for (const consumer of this._consumers.values()) {
      consumer.push(value);
    }
  }
  listen<ERROR>(
    handler: Consumer.Handler<VALUE, ERROR, NAME>,
    init?: Prettify<Omit<Consumer.Init<VALUE, ERROR, NAME>, "handler" | "name">>,
  ): Consumer<VALUE, ERROR, NAME>;
  listen<ERROR>(init: Prettify<Omit<Consumer.Init<VALUE, ERROR, NAME>, "name">>): Consumer<VALUE, ERROR, NAME>;
  listen<ERROR>(
    handlerOrInit: Consumer.Handler<VALUE, ERROR, NAME> | Prettify<Omit<Consumer.Init<VALUE, ERROR, NAME>, "name">>,
    _init?: Omit<Consumer.Init<VALUE, ERROR, NAME>, "handler">,
  ): Consumer<VALUE, ERROR, NAME> {
    const init = typeof handlerOrInit === "function" ? { handler: handlerOrInit, ..._init } : { ...handlerOrInit };

    if (this._consumers.has(init.handler)) return this._consumers.get(init.handler)!;

    const consumer = new Consumer({
      ...init,
      name: this.name,
      onEvent: (event) => {
        switch (event.type) {
          case "abort":
          case "complete":
            this._consumers.delete(init.handler);
            this._fireEvent({ type: "consumer-left", consumer });

            if (this._consumers.size === 0 && this._state === "drain") {
              this.completed();
            }
            break;
          case "ready":
            if (!this._pulling && this._sourceConsumer) {
              this._pulling = true;
              this._sourceConsumer.next();
            }
            break;
          case "error":
            this._fireEvent({ type: "error", error: event.error });
        }

        init.onEvent?.(event);
      },

      queue: init.queue ? init.queue : this._queueFactory?.(),
    });

    this._consumers.set(init.handler, consumer);
    this._fireEvent({ type: "consumer-join", consumer });

    if (init.isReady !== false && !this._pulling && this._sourceConsumer) {
      this._pulling = true;
      this._sourceConsumer.next();
    }

    return consumer;
  }
  abort(error?: any): void {
    if (this._state === "aborted" || this._state === "completed") return;
    this._state = "aborted";
    this.push = () => this._fireEvent({ type: "error", error: new Smoker.Exception("push_not_allowed", "aborted") });
    for (const consumer of this._consumers.values()) {
      consumer.abort(error);
    }
    this._fireEvent({ type: "abort", error });

    this.clean("aborted", error);
  }
  complete(): void {
    if (this._state !== "active") return;

    if (this._consumers.size) {
      this._state = "drain";
      this.push = () => this._fireEvent({ type: "error", error: new Smoker.Exception("push_not_allowed", "drain") });
      this._fireEvent({ type: "drain" });
    } else {
      this.completed();
    }
    for (const consumer of this._consumers.values()) {
      consumer.complete();
    }
  }
  private completed(): void {
    this._state = "completed";
    this.push = () => this._fireEvent({ type: "error", error: new Smoker.Exception("push_not_allowed", "completed") });
    this._fireEvent({ type: "complete" });
    this.clean("completed");
  }
  private clean(reason: "aborted" | "completed", error?: any): void {
    this._event?.complete();
    reason === "aborted" ? this._sourceConsumer?.abort(error) : this._sourceConsumer?.complete();
    this._event = this._queueFactory = this._sourceConsumer = undefined;
  }
  get<PROP extends "state" | "consumersCount" | "event" | "queueFactory">(
    prop: PROP,
  ): PROP extends "state"
    ? Smoker.State
    : PROP extends "consumersCount"
      ? number
      : PROP extends "event"
        ? Smoker<Smoker.Event<VALUE, NAME>, `${NAME}Event`>
        : PROP extends "queueFactory"
          ? Smoker.QueueFactory<VALUE>
          : never {
    switch (prop) {
      case "state":
        return this._state as never;
      case "consumersCount":
        return this._consumers.size as never;
      case "event":
        if (!this._event) this._event = new Smoker({ name: `${this.name}Event` });
        return new Smoker({ name: this._event.name, source: this._event }) as never;
      case "queueFactory":
        return this._queueFactory as never;
    }
  }
  pipe<OUT_NAME extends string, OUT extends Transformer<this, any, OUT_NAME> | this>(
    transform: Smoker.Transform<this, OUT_NAME, OUT>,
  ): OUT;
  pipe<OUT_NAME extends string, OUT extends Transformer<this, any, OUT_NAME> | this>(
    name: OUT_NAME,
    transform: Smoker.Transform<this, OUT_NAME, OUT>,
  ): OUT;
  pipe<OUT_NAME extends string, OUT extends Transformer<this, any, OUT_NAME> | this>(
    nameOrTransform: OUT_NAME | Smoker.Transform<this, OUT_NAME, OUT>,
    transform?: Smoker.Transform<this, OUT_NAME, OUT>,
  ): OUT {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }

  static fromIterable<VALUE>(iterable: Iterable<VALUE>) {
    //TODO
  }
}

export namespace Smoker {
  export const NAME = "root";
  export type Name = typeof NAME;
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnySmoker = Smoker<any, any>;
  export type Scope =
    | AnySmoker
    | { any: [AnySmoker, ...AnySmoker[]]; all?: never }
    | { any?: never; all: [AnySmoker, ...AnySmoker[]] };

  export type Event<VALUE, NAME extends string> =
    | EventShape<"drain" | "complete">
    | EventShape<"abort" | "error", { error: any }>
    | EventShape<"consumer-join" | "consumer-left", { consumer: Consumer<VALUE, any, NAME> }>;

  export type OnEvent<VALUE, NAME extends string> = (event: Event<VALUE, NAME>) => void;
  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Init<VALUE, NAME extends string> = {
    name?: NAME;
    source?: Smoker<VALUE, any> | Consumer<VALUE, any, any>;
    scope?: Scope;
    onEvent?: OnEvent<VALUE, NAME>;
    queueFactory?: QueueFactory<VALUE>;
  };
  export type ExtractValue<T extends AnySmoker | Transformer.AnyTransformer> =
    T extends Smoker<infer VALUE, any>
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
  export class Exception extends Error {
    override cause?: "aborted" | "completed" | "drain";
    override message: "push_not_allowed";
    constructor(message: "push_not_allowed", cause: "aborted" | "completed" | "drain") {
      super();
      this.cause = cause;
      this.message = message;
    }
  }
}

function bench() {
  const MAX = 10_000_000;
  const smoker = new Smoker<number>();

  const consumer = smoker.listen(
    (value, consumer) => {
      if (value === MAX) console.log("foo", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      const obj = { value, consumer };
      obj.value++;
      // if (value === 1000) {
      //   queueMicrotask(() => {
      //     console.log("promise resolved", value);
      //     consumer.next();
      //   });
      //   return;
      // }

      consumer.next();
    },
    { isReady: false },
  );

  for (let i = 0; i <= MAX; i++) {
    smoker.push(i);
  }
  const start = performance.now();
  consumer.next();
}

// bench(); //foo 10 000 000 46 ms

function sequential() {
  const smoker = new Smoker<number>();

  smoker.listen(async (value, consumer) => {
    await new Promise((r) => setTimeout(r, Math.random() * 1000));
    console.log("sequential", value);
    consumer.next();
  });

  smoker.push(1);
  smoker.push(2);
  smoker.push(3);
}

// sequential();
function concurrent() {
  const smoker = new Smoker<number>();

  smoker.listen(async (value, consumer) => {
    consumer.next();
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
  const smoker = new Smoker<number>();
  smoker.get("event").listen((e, consumer) => {
    if (e.type === "error") {
      console.log(e.error);
    }
    consumer.next();
  });

  smoker.listen((value, consumer) => {
    if (value === 3) throw "kechmahaja";
    console.log(value);

    consumer.next();
  });

  smoker.push(1);
  smoker.push(2);
  smoker.push(3);
}

// errorHandling();
