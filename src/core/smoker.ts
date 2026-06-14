import { Consumer } from "./consumer";
import type { Prettify, Source } from "./types";

export class Smoker<VALUE, NAME extends string = Smoker.Name> implements Source<VALUE> {
  readonly name: NAME;
  private _consumers = new Set<Consumer<VALUE, any>>();
  private _gloablError?: Smoker<any>;

  constructor(name?: NAME) {
    this.name = name ?? (Smoker.NAME as NAME);
  }

  emit(value: VALUE) {
    for (const consumer of this._consumers) {
      consumer.push(value);
    }
  }

  listen<ERROR>(
    handler: Source.Handler<VALUE, ERROR>,
    init?: Prettify<Omit<Consumer.Init<VALUE, ERROR>, "handler">>,
  ): Source.Abort<ERROR>;
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Source.Abort<ERROR>;
  listen<ERROR>(
    handlerOrInit: Source.Handler<VALUE, ERROR> | Consumer.Init<VALUE, ERROR>,
    _init?: Omit<Consumer.Init<VALUE, ERROR>, "handler">,
  ): Source.Abort<ERROR> {
    const init = typeof handlerOrInit === "function" ? { handler: handlerOrInit, ..._init } : { ...handlerOrInit };

    const consumer = new Consumer({
      ...init,
      abort: (error) => {
        this._consumers.delete(consumer);
        init?.abort?.(error);
      },
    });

    this._consumers.add(consumer);

    return consumer.abort;
  }

  clear() {
    this._consumers.clear();
    this._gloablError?.clear();
    this._gloablError = undefined;
  }

  get consumers() {
    return this._consumers.size;
  }
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
  };
}

function bench() {
  const MAX = 40_000_000;
  const smoker = new Smoker<number>();

  const start = performance.now();
  // smoker.listen(() => {});
  smoker.listen((value, ready) => {
    if (value === MAX) console.log("foo", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
    const obj = { value, ready };
    obj.value++;
    if (value === 1000) {
      value++;
      // queueMicrotask(() => {
      //   console.log("promise resolved", value);
      //   ready();
      // });
      // return;
    }
    ready();
  });

  for (let i = 0; i <= MAX; i++) {
    smoker.emit(i);
  }
}

bench(); //foo 40 000 000 937 ms

function sequential() {
  const smoker = new Smoker<number>();

  smoker.listen(async (value, ready) => {
    await new Promise((r) => setTimeout(r, Math.random() * 1000));
    console.log("sequential", value);
    ready();
  });

  smoker.emit(1);
  smoker.emit(2);
  smoker.emit(3);
}

// sequential();
function consurrent() {
  const smoker = new Smoker<number>();

  smoker.listen(async (value, ready) => {
    ready();
    await new Promise((r) => setTimeout(r, Math.random() * 1000));
    console.log("concurrent", value);
  });

  smoker.emit(1);
  smoker.emit(2);
  smoker.emit(3);
}

// consurrent();
