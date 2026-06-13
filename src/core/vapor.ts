import { Subscription } from "./subscription";

export class Vapor<VALUE> {
  private _subscriptions = new Set<Subscription<VALUE, any>>();
  private _gloablError?: Vapor<any>;

  constructor() {
    this._subscriptions = new Set();
  }

  emit(value: VALUE) {
    for (const sub of this._subscriptions) {
      sub.push(value);
    }
  }

  listen<ERROR>(
    listener: Subscription.Listener<VALUE, ERROR>,
    init?: Omit<Subscription.Init<VALUE, ERROR>, "listener">,
  ): Subscription.Abort<ERROR>;
  listen<ERROR>(init: Subscription.Init<VALUE, ERROR>): Subscription.Abort<ERROR>;
  listen<ERROR>(
    listenerOrInit: Subscription.Listener<VALUE, ERROR> | Subscription.Init<VALUE, ERROR>,
    _init?: Omit<Subscription.Init<VALUE, ERROR>, "listener">,
  ): Subscription.Abort<ERROR> {
    const init = typeof listenerOrInit === "function" ? { listener: listenerOrInit, ..._init } : { ...listenerOrInit };

    const sub = new Subscription({
      ...init,
      abort: (error) => {
        this._subscriptions.clear();
        init?.abort?.(error);
      },
    });

    this._subscriptions.add(sub);

    return sub.abort;
  }

  clear() {
    this._subscriptions.clear();
    this._gloablError?.clear();
    this._gloablError = undefined;
  }

  get subscriptionsCount() {
    return this._subscriptions.size;
  }
}

export namespace Vapor {}

function bench() {
  const MAX = 50_000_000;
  const vapor = new Vapor<number>();

  const start = performance.now();

  vapor.listen(({ value, ready }) => {
    if (value === MAX) console.log("foo", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
    const obj = { value, ready };
    obj.value++;
    // if (value === 1000) {
    //   queueMicrotask(() => {
    //     console.log("promise resolved", value);
    //     ready();
    //   });
    //   return;
    // }
    ready();
  });

  for (let i = 0; i <= MAX; i++) {
    vapor.emit(i);
  }
}

bench(); //foo 8 000 000 959 ms

function sequential() {
  const vapor = new Vapor<number>();

  vapor.listen(async ({ value, ready }) => {
    await new Promise((r) => setTimeout(r, Math.random() * 1000));
    console.log("sequential", value);
    ready();
  });

  vapor.emit(1);
  vapor.emit(2);
  vapor.emit(3);
}

// sequential();
function consurrent() {
  const vapor = new Vapor<number>();

  vapor.listen(async ({ value, ready }) => {
    ready();
    await new Promise((r) => setTimeout(r, Math.random() * 1000));
    console.log("concurrent", value);
  });

  vapor.emit(1);
  vapor.emit(2);
  vapor.emit(3);
}

// consurrent();
