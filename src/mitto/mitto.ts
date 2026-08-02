import { sleep } from "bun";

export class Mitto<VALUE> {
  private _listeners: [Mitto.Listener<VALUE>][] = [];
  private _listenersCount = 0;
  private _threshold: number;
  private _abortSignal?: Mitto.Abort;
  private _abortSource?: Mitto.Abort;
  private _$close?: Mitto<void>;
  private _$mitto?: Mitto<VALUE>;
  constructor(private options?: Mitto.Options<VALUE>) {
    const { $source, $signal, threshold = 5 } = options ?? {};
    this._threshold = threshold > 0 ? threshold : 5;

    if ($signal) {
      this._abortSignal = $signal.listen(() => this.close());
    }

    if ($source) {
      this._abortSource = $source.listen((v) => this.push(v));
      $source._$close ??= new Mitto();
      $source._$close.listen(() => this.close());
    }
  }
  get listenersCount() {
    return this._listenersCount;
  }
  get $close() {
    this._$close ??= new Mitto();
    return new Mitto({ $source: this._$close });
  }

  push(value: VALUE) {
    const listeners = this._listeners;
    const len = listeners.length;

    for (let i = 0; i < len; i++) {
      listeners[i][0](value);
    }
    this._$mitto?.push(value);
  }

  listen(listener: Mitto.Listener<VALUE>, $signal?: Mitto<any>): Mitto.Abort {
    if (this._listeners.length >= this._threshold) {
      this._$mitto ??= new Mitto<VALUE>({ threshold: this._threshold });

      return this._$mitto.listen(listener, $signal);
    }
    const abortSignal = $signal?.listen(() => abort());

    const entry = [listener] as [Mitto.Listener<VALUE>];
    this._listeners.push(entry);
    this._listenersCount++;

    this.options?.listenerAdded?.(this, listener);

    const self = this;

    return abort;

    function abort() {
      entry[0] = Mitto.EMPTY_FUNCTION;
      self._listenersCount--;
      if (self._listenersCount === 0) {
        if (self._$mitto) {
          self._listeners = self._$mitto._listeners;
          self._listenersCount = self._$mitto._listenersCount;
          self._$mitto = self._$mitto._$mitto;
        } else {
          self._listeners = [];
        }
      }
      self.options?.listenerRemoved?.(self, listener);
      abortSignal?.();
    }
  }
  close() {
    this._listeners.length = 0;
    this._listenersCount = 0;
    this._abortSignal?.();
    this._abortSource?.();
    this._$close?.push();
    this._$close?.close();
    this._$mitto?.close();
    this._$close = this._$mitto = this._abortSignal = this._abortSource = undefined;
    this.options?.close?.(this);
  }
  pipe<OUTPUT>(transform: (input: Mitto<VALUE>) => Mitto<OUTPUT>): Mitto<OUTPUT> {
    return transform(this);
  }
}

export namespace Mitto {
  export type Listener<VALUE> = (value: VALUE) => void;
  export type Abort = () => void;
  export type Options<VALUE> = {
    threshold?: number;
    $source?: Mitto<VALUE>;
    $signal?: Mitto<any>;
    close?: (self: Mitto<VALUE>) => void;
    listenerAdded?: (self: Mitto<VALUE>, listener: Listener<VALUE>) => void;
    listenerRemoved?: (self: Mitto<VALUE>, listener: Listener<VALUE>) => void;
  };
  export const EMPTY_FUNCTION = () => {};
}
export const map = <T, R>(fn: (val: T) => R) => {
  return (source: Mitto<T>): Mitto<R> => {
    const destination = new Mitto<R>();

    // Links upstream data emissions straight through the functional transform
    const unsub = source.listen((val) => destination.push(fn(val)));

    // Automatically sever pipeline links if the output node is explicitly closed
    destination.$close.listen(() => unsub());

    return destination;
  };
};

export const filter = <T>(predicate: (val: T) => boolean) => {
  return (source: Mitto<T>): Mitto<T> => {
    const destination = new Mitto<T>();
    const unsub = source.listen((val) => {
      if (predicate(val)) destination.push(val);
    });
    destination.$close.listen(() => unsub());
    return destination;
  };
};
function bench() {
  const MAX = 20_000_000;

  const start = performance.now();

  const mitto = new Mitto();

  mitto
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .listen((v) => {
      if (v === MAX) console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
    });

  for (let i = 0; i <= MAX; i++) {
    mitto.push(i);
  }
}
bench(); //20 000 000 1642 ms

function test() {
  const $source = new Mitto<number>();
  const $mitto = new Mitto({ $source, threshold: 2 });

  $mitto.listen((v) => console.log("c1", v));
  $mitto.listen((v) => console.log("c2", v));
  $mitto.listen((v) => console.log("c3", v));
  $mitto.listen((v) => console.log("c4", v));

  $source.push(1);
  //   $source.push(2);
  //   $source.push(3);
}

// test();
// c1 1
// c2 1
// c3 1
// c4 1
// c1 2
// c2 2
// c3 2
// c4 2
// c1 3
// c2 3
// c3 3
// c4 3

function transformersTest() {
  const numbers$ = new Mitto<number>();

  // 100% Isolated pipeline instances. Each has exactly 1 listener.
  const processedStream$ = numbers$.pipe(filter((x) => x % 2 === 0)).pipe(map((x) => x * 10));

  // The terminal subscriber
  processedStream$.listen((val) => console.log("Emitted:", val));

  numbers$.push(1); // Blocked by filter
  numbers$.push(2); // Passes filter -> Maps 2 to 20 -> Prints "Emitted: 20"
}
// transformersTest();

function churnBench() {
  const TOTAL_EVENTS = 10000;
  const mitto = new Mitto<number>({ threshold: 10 });

  // 1. Maintain a steady state right up to the threshold boundary
  for (let i = 0; i < 4; i++) {
    mitto.listen(() => {});
  }

  let receivedCount = 0;
  const start = performance.now();

  for (let i = 0; i < TOTAL_EVENTS; i++) {
    // 2. Rapidly flood past the threshold with short-lived subscribers
    const unsub1 = mitto.listen((v) => {
      receivedCount++;
    });
    const unsub2 = mitto.listen((v) => {
      receivedCount++;
    });

    // 3. Fire the event (triggers branch traversal)
    mitto.push(i);

    // 4. Immediately destroy the branch via unsubscription
    unsub1();
    unsub2();
  }

  const duration = Math.round(performance.now() - start);
  console.log(`Processed ${TOTAL_EVENTS.toLocaleString()} events with massive churn.`);
  console.log(`Total callbacks triggered: ${receivedCount.toLocaleString()}`);
  console.log(`Time taken: ${duration} ms`);
}
// churnBench();
