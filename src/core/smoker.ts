import { Consumer } from "./consumer";
import type { Prettify, Queue, Source, EventShape } from "./types";

export class Smoker<VALUE, NAME extends string = Smoker.Name> {
  private _consumers = new Map<Consumer.Handler<VALUE, any, NAME>, Consumer<VALUE, any, NAME>>();
  private _state: Smoker.State;
  private _name: NAME;
  private _sourceConsumer?: Consumer<VALUE, any, any>;
  private _fireEvent: Smoker.OnEvent<VALUE>;
  private _queueFactory?: Smoker.QueueFactory<VALUE>;
  private _event?: Smoker<Smoker.Event<VALUE>, `${NAME}Event`>;
  private _pulling = false;
  constructor(init?: Smoker.Init<VALUE, NAME>) {
    this._name = init?.name ?? (Smoker.NAME as NAME);
    this._fireEvent = (event: Smoker.Event<VALUE>) => {
      init?.onEvent?.(event);
      this._event?.push(event);
    };
    this._queueFactory = init?.queueFactory;

    this._state = "active";

    if (init?.source) {
      this._sourceConsumer = init?.source.listen({
        handler: (value) => {
          this._pulling = false;
          this.push(value);
        },
        isReady: false,
      });
    }
  }

  push(value: VALUE): void {
    for (const consumer of this._consumers.values()) {
      consumer.push(value);
    }
  }
  listen<ERROR>(
    handler: Consumer.Handler<VALUE, ERROR, NAME>,
    init?: Prettify<Omit<Consumer.Init<VALUE, ERROR, NAME>, "handler">>,
  ): Consumer<VALUE, ERROR, any>;
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR, NAME>): Consumer<VALUE, ERROR, NAME>;
  listen<ERROR>(
    handlerOrInit: Consumer.Handler<VALUE, ERROR, NAME> | Consumer.Init<VALUE, ERROR, NAME>,
    _init?: Omit<Consumer.Init<VALUE, ERROR, NAME>, "handler">,
  ): Consumer<VALUE, ERROR, NAME> {
    const init = typeof handlerOrInit === "function" ? { handler: handlerOrInit, ..._init } : { ...handlerOrInit };

    if (this._consumers.has(init.handler)) return this._consumers.get(init.handler)!;

    const consumer = new Consumer({
      ...init,
      onEvent: (event) => {
        if (event.type === "complete" || event.type === "abort") {
          this._consumers.delete(init.handler);
          this._fireEvent({ type: "consumer-left", consumer });

          if (this._consumers.size === 0 && this._state === "drain") {
            this.completed();
          }
        }
        if (event.type === "ready" && !this._pulling && this._sourceConsumer) {
          this._pulling = true;
          this._sourceConsumer.next();
        } else if (event.type === "error") {
          this._fireEvent({ type: "error", error: event.error });
        }

        init.onEvent?.(event);
      },

      queue: init.queue ? init.queue : this._queueFactory?.(),
    });

    this._consumers.set(init.handler, consumer);
    this._fireEvent({ type: "consumer-join", consumer });

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
  get<PROP extends "name" | "state" | "consumersCount" | "event" | "queueFactory">(
    prop: PROP,
  ): PROP extends "name"
    ? NAME
    : PROP extends "state"
      ? Smoker.State
      : PROP extends "consumersCount"
        ? number
        : PROP extends "event"
          ? Smoker<Smoker.Event<VALUE>, `${NAME}Event`>
          : PROP extends "queueFactory"
            ? Smoker.QueueFactory<VALUE>
            : never {
    switch (prop) {
      case "name":
        return this._name as never;
      case "state":
        return this._state as never;
      case "consumersCount":
        return this._consumers.size as never;
      case "event":
        if (!this._event) this._event = new Smoker({ name: `${this._name}Event` });
        return new Smoker({ name: this._event.get("name"), source: this._event }) as never;
      case "queueFactory":
        return this._queueFactory as never;
    }
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

  export type Event<VALUE> =
    | EventShape<"drain" | "complete">
    | EventShape<"abort" | "error", { error: any }>
    | EventShape<"consumer-join" | "consumer-left", { consumer: Consumer<VALUE, any, any> }>;

  export type OnEvent<VALUE> = (event: Event<VALUE>) => void;
  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Init<VALUE, NAME extends string> = {
    name?: NAME;
    source?: Source<VALUE>;
    scope?: Scope;
    onEvent?: OnEvent<VALUE>;
    queueFactory?: QueueFactory<VALUE>;
  };

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
  smoker.get("event").listen((e) => {
    console.log(e);
    // if (e.type === "error") {
    // console.log(e.data);
    // }
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

errorHandling();
