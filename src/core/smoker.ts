import { Consumer } from "./consumer";
import type { Prettify, Queue, Source } from "./types";

export class Smoker<VALUE, NAME extends string = Smoker.Name> {
  private _consumers = new Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>();
  private _state: Smoker.State;
  private _name: NAME;
  private _source?: Consumer<VALUE, any>;
  private _globalError?: Smoker.AnySmoker;
  private _drain?: Smoker.Drain;
  private _complete?: Smoker.Complete;
  private _abort?: Smoker.Abort;
  private _error?: Smoker.Error;
  private _queueFactory?: Smoker.QueueFactory<VALUE>;

  private _events?: Smoker.Events<VALUE, NAME>;
  constructor(init?: Smoker.Init<VALUE, NAME>) {
    this._name = init?.name ?? (Smoker.NAME as NAME);
    this._globalError = init?.globalError;
    this._drain = init?.drain;
    this._complete = init?.complete;
    this._abort = init?.abort;
    this._error = init?.error;
    this._queueFactory = init?.queueFactory;

    this._events = init?.globalError ? { globalError: init.globalError } : undefined;
    this._state = "active";

    if (init?.source) {
      this._source = init?.source.listen({
        handler: (value) => {
          this.push(value);
        },
        isReady: false,
      });
    }
  }

  push(value: VALUE) {
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
      complete: () => {
        this._consumers.delete(init.handler);
        if (this._consumers.size === 0 && this._state === "drain") {
          this.completed();
        }
        init.complete?.();
        this._events?.consumerLeave?.push(consumer);
      },
      abort: (error) => {
        this._consumers.delete(init.handler);
        init?.abort?.(error);
        this._events?.consumerLeave?.push(consumer);
      },
      queue: init.queue ? init.queue : this._queueFactory?.(),
      globalError: init.globalError ? init.globalError : this._events?.globalError,
    });

    this._consumers.set(init.handler, consumer);
    this._events?.consumerJoin?.push(consumer);

    return consumer;
  }
  abort(error?: any): void {
    if (this._state === "aborted" || this._state === "completed") return;
    this._state = "aborted";
    this.push = () => this.error(new Smoker.Exception("push_not_allowed", "aborted"));
    for (const consumer of this._consumers.values()) {
      consumer.abort(error);
    }
    this._abort?.(error);
    this._events?.abort?.push(error);
    this.clean(error);
  }
  complete(): void {
    if (this._state !== "active") return;

    if (this._consumers.size) {
      this._state = "drain";
      this.push = () => this.error(new Smoker.Exception("push_not_allowed", "drain"));
      this._drain?.();
      this._events?.drain?.push();
    } else {
      this.completed();
    }
    for (const consumer of this._consumers.values()) {
      consumer.complete();
    }
  }
  private completed() {
    this._state = "completed";
    this.push = () => this.error(new Smoker.Exception("push_not_allowed", "completed"));
    this._complete?.();
    this._events?.complete?.push();
    this.clean();
  }
  private error(error: any): void {
    if (!this._events?.globalError?.get("consumersCount") && !this._error) {
      Promise.reject(error);
    } else {
      this._events?.globalError?.push(error);
      this._error?.(error);
    }
  }
  private clean(error?: any) {
    this._events?.globalError?.push(error);
    this._events?.globalError?.complete();
    this._events?.drain?.complete();
    this._events?.complete?.complete();
    this._events?.abort?.complete();
    this._events?.consumerJoin?.complete();
    this._events?.consumerLeave?.complete();
    this._events = undefined;
  }

  get<PROP extends "name" | "state" | "consumersCount" | NonNullable<keyof Smoker.Events<VALUE, NAME>>>(
    prop: PROP,
  ): PROP extends "name"
    ? NAME
    : PROP extends "state"
      ? Smoker.State
      : PROP extends "consumersCount"
        ? number
        : PROP extends NonNullable<keyof Smoker.Events<VALUE, NAME>>
          ? NonNullable<Smoker.Events<VALUE, NAME>[PROP]>
          : never {
    switch (prop) {
      case "name":
        return this._name as never;
      case "state":
        return this._state as never;
      case "consumersCount":
        return this._consumers.size as never;
      case "drain":
        if (!this._events) this._events = {};
        if (!this._events.drain) this._events.drain = new Smoker({ name: `${this._name}Drain` });
        return new Smoker({ name: this._events.drain.get("name"), source: this._events.drain }) as never;
      case "complete":
        if (!this._events) this._events = {};
        if (!this._events.complete) this._events.complete = new Smoker({ name: `${this._name}Complete` });
        return new Smoker({ name: this._events.complete.get("name"), source: this._events.complete }) as never;
      case "abort":
        if (!this._events) this._events = {};
        if (!this._events.abort) this._events.abort = new Smoker({ name: `${this._name}Abort` });
        return new Smoker({ name: this._events.abort.get("name"), source: this._events.abort }) as never;
      case "consumerJoin":
        if (!this._events) this._events = {};
        if (!this._events.consumerJoin) this._events.consumerJoin = new Smoker({ name: `${this._name}ConsumerJoin` });
        return new Smoker({ name: this._events.consumerJoin.get("name"), source: this._events.consumerJoin }) as never;
      case "consumerLeave":
        if (!this._events) this._events = {};
        if (!this._events.consumerLeave)
          this._events.consumerLeave = new Smoker({ name: `${this._name}ConsumerLeave` });
        return new Smoker({
          name: this._events.consumerLeave.get("name"),
          source: this._events.consumerLeave,
        }) as never;
      case "globalError":
        if (!this._events) this._events = {};
        if (!this._events.globalError) this._events.globalError = new Smoker({ name: `${this._name}GlobalError` });
        return new Smoker({
          name: this._events.globalError.get("name"),
          source: this._events.globalError,
        }) as never;
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

  export type Abort = (error?: any) => void;
  export type Drain = () => void;
  export type Complete = () => void;
  export type Error = (error: any) => void;
  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Init<VALUE, NAME extends string> = {
    name?: NAME;
    source?: Source<VALUE>;
    scope?: Scope;
    globalError?: AnySmoker;
    drain?: Drain;
    complete?: Complete;
    abort?: Abort;
    error?: Error;
    queueFactory?: QueueFactory<VALUE>;
  };

  export type Events<VALUE, NAME extends string> = {
    drain?: Smoker<void, `${NAME}Drain`>;
    complete?: Smoker<void, `${NAME}Complete`>;
    abort?: Smoker<any, `${NAME}Abort`>;
    consumerJoin?: Smoker<Consumer<VALUE, any>, `${NAME}ConsumerJoin`>;
    consumerLeave?: Smoker<Consumer<VALUE, any>, `${NAME}ConsumerLeave`>;
    globalError?: AnySmoker;
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

bench(); //foo 10 000 000 46 ms

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
