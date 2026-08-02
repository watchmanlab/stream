export class Mitto<VALUE> {
  private _listener: Mitto.Listener<VALUE> | Mitto.EmptyFunction = Mitto.EMPTY_FUNCTION;
  private _$nextMitto?: Mitto<VALUE>;
  private _$tailMitto: Mitto<VALUE> = this;
  private _abortSignal?: Mitto.Abort;
  private _abortSource?: Mitto.Abort;
  private _$clear?: Mitto<void>;

  constructor(private options?: Mitto.Options<VALUE>) {
    const { $source, $signal } = options ?? {};

    if ($signal) {
      this._abortSignal = $signal.listen(() => this.clear());
    }

    if ($source) {
      this._abortSource = $source.listen((v) => this.push(v));
      $source._$clear ??= new Mitto();
      $source._$clear.listen(() => this.clear());
    }
  }

  get hasListenrs() {
    return this._listener !== Mitto.EMPTY_FUNCTION;
  }

  get $clear() {
    this._$clear ??= new Mitto();
    return new Mitto({ $source: this._$clear });
  }

  push(value: VALUE) {
    this._listener(value);

    if (this._$nextMitto) {
      let mitto: Mitto<VALUE> | undefined = this._$nextMitto;
      while (mitto) {
        mitto._listener(value);
        mitto = mitto._$nextMitto;
      }
    }
  }

  listen(listener: Mitto.Listener<VALUE>, $signal?: Mitto<any>): Mitto.Abort {
    if (this._listener !== Mitto.EMPTY_FUNCTION) {
      const $tailMitto = this._$tailMitto;
      $tailMitto._$nextMitto = new Mitto<VALUE>();
      this._$tailMitto = $tailMitto._$nextMitto;
      return $tailMitto._$nextMitto.listen(listener, $signal);
    }

    const abortSignal = $signal?.listen(() => abort());
    this._listener = listener;
    this.options?.listenerAdded?.(this, listener);

    const self = this;
    return abort;

    function abort() {
      if (self._listener === Mitto.EMPTY_FUNCTION) return;
      self._listener = Mitto.EMPTY_FUNCTION;

      if (self._$nextMitto) {
        console.log("dd");
        self._listener = self._$nextMitto._listener;

        const deadChild = self._$nextMitto;
        self._$nextMitto = self._$nextMitto._$nextMitto;

        deadChild._$nextMitto = undefined;
        deadChild._listener = Mitto.EMPTY_FUNCTION;
      }

      // Lazy Tail Reset: If the terminal node itself is being removed,
      // the master tail reference is temporarily broken.
      // We set the next subscription path to reset itself lazily.
      // (Handled cleanly because the next listen call will evaluate the head state)
      self.options?.listenerRemoved?.(self, listener);

      abortSignal?.();
    }
  }

  clear() {
    this._abortSignal?.();
    this._abortSource?.();
    this._$clear?.push();
    this._$clear?.clear();
    this._$nextMitto?.clear();
    this._$clear = this._$nextMitto = this._abortSignal = this._abortSource = undefined;
    this._listener = Mitto.EMPTY_FUNCTION;
    this._$tailMitto = this;
    this.options?.clear?.(this);
  }

  pipe<OUTPUT>(transform: (input: Mitto<VALUE>) => Mitto<OUTPUT>): Mitto<OUTPUT> {
    return transform(this);
  }
}

export namespace Mitto {
  export type Listener<VALUE> = (value: VALUE) => void;
  export type Abort = () => void;
  export type Options<VALUE> = {
    $source?: Mitto<VALUE>;
    $signal?: Mitto<any>;
    clear?: (self: Mitto<VALUE>) => void;
    listenerAdded?: (self: Mitto<VALUE>, listener: Listener<VALUE>) => void;
    listenerRemoved?: (self: Mitto<VALUE>, listener: Listener<VALUE>) => void;
  };
  export const EMPTY_FUNCTION = () => {};
  export type EmptyFunction = typeof EMPTY_FUNCTION;
}
export const map = <T, R>(fn: (val: T) => R) => {
  return (source: Mitto<T>): Mitto<R> => {
    const destination = new Mitto<R>();

    // Links upstream data emissions straight through the functional transform
    const unsub = source.listen((val) => destination.push(fn(val)));

    // Automatically sever pipeline links if the output node is explicitly closed
    destination.$clear.listen(() => unsub());

    return destination;
  };
};

export const filter = <T>(predicate: (val: T) => boolean) => {
  return (source: Mitto<T>): Mitto<T> => {
    const destination = new Mitto<T>();
    const unsub = source.listen((val) => {
      if (predicate(val)) destination.push(val);
    });
    destination.$clear.listen(() => unsub());
    return destination;
  };
};
function bench() {
  const MAX = 30_000_000;

  const start = performance.now();

  const mitto = new Mitto();

  mitto
    // .pipe(map((v) => v))
    // .pipe(map((v) => v))
    // .pipe(map((v) => v))
    // .pipe(map((v) => v))
    // .pipe(map((v) => v))
    .listen((v) => {
      if (v === MAX) console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
    });

  for (let i = 0; i <= MAX; i++) {
    mitto.push(i);
  }
}
// bench(); //1 000 000 37 ms

function test() {
  const $source = new Mitto<number>();
  const $mitto = new Mitto({ $source });

  const abort1 = $mitto.listen((v) => console.log("c1", v));
  const abort2 = $mitto.listen((v) => console.log("c2", v));
  //   const abort3 = $mitto.listen((v) => console.log("c3", v));
  //   const abort4 = $mitto.listen((v) => console.log("c4", v));

  $source.push(1);

  abort1();
  abort2();
  //   abort3();
  //   abort4();

  console.log($mitto.hasListenrs);

  //   $source.push(2);
  //   $source.push(3);
}

test();

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
  const mitto = new Mitto<number>();

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
