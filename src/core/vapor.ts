import { Subscription } from "./subscription";

export class Vapor<VALUE> {
  private _subscriptions: Set<Subscription<VALUE>>;

  constructor() {
    this._subscriptions = new Set();
  }

  emit(value: VALUE) {
    for (const sub of this._subscriptions) {
      sub.push(value);
    }
  }

  listen(listener: Subscription.Listener<VALUE>): Subscription.Abort {
    const subscriptions = this._subscriptions;

    const sub = new Subscription({ listener, onAbort: abort });

    this._subscriptions.add(sub);

    return abort;

    function abort() {
      subscriptions.delete(sub);
    }
  }

  clear() {
    this._subscriptions.clear();
  }

  get subscriptionsCount() {
    return this._subscriptions.size;
  }
}

export namespace Vapor {}

function bench() {
  const MAX = 40_000_000;
  const vapor = new Vapor<number>();

  const start = performance.now();

  vapor.listen(({ value, ready }) => {
    if (value === MAX) console.log("foo", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");

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

bench(); //foo 100 000 000  785 ms

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
