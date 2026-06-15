import { Consumer } from "./consumer";
import type { Prettify, Queue, Source } from "./types";

export class Smoker<VALUE, NAME extends string = Smoker.Name> implements Source<VALUE> {
  readonly name: NAME;
  private _consumers = new Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>();
  private _globalError?: Smoker<any>;
  private _source?: Consumer<VALUE, any>;

  constructor(private options?: Smoker.Options<VALUE, NAME>) {
    this.name = options?.name ?? (Smoker.NAME as NAME);

    if (options?.source) {
      this._source = options.source.listen({
        handler: (value) => {
          this.emit(value);
        },
      });
    }
  }

  emit(value: VALUE) {
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
      abort: (error) => {
        this._consumers.delete(init.handler);
        init?.abort?.(error);
      },
      complete: () => {
        this._consumers.delete(init.handler);
        init.complete?.();
      },
      queue: init.queue ? init.queue : this.options?.queue?.(),
      globalError: init.globalError ? init.globalError : this._globalError,
    });

    this._consumers.set(init.handler, consumer);

    return consumer;
  }
  clear() {
    this._consumers.clear();
    this._globalError?.clear();
    this._globalError = undefined;
  }
  get consumers() {
    return this._consumers.size;
  }
  static fromIterable<VALUE>(iterable: Iterable<VALUE>) {}
}

export namespace Smoker {
  export const NAME = "root";
  export type Name = typeof NAME;
  export type AnySmoker = Smoker<any, any>;
  export type Scope =
    | AnySmoker
    | { any: [AnySmoker, ...AnySmoker[]]; all?: never }
    | { any?: never; all: [AnySmoker, ...AnySmoker[]] };

  export type Options<VALUE, NAME extends string> = {
    name?: NAME;
    source?: Source<VALUE>;
    scope?: Scope;
    error?: Smoker<any>;
    queue?: () => Queue<VALUE>;
  };
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
    smoker.emit(i);
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

  smoker.emit(1);
  smoker.emit(2);
  smoker.emit(3);
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

  smoker.emit(1);
  smoker.emit(2);
  smoker.emit(3);
}

// concurrent();
