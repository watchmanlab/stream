export class Mitto<VALUE> {
  private _listeners: [Mitto.Listener<VALUE>][] = [];
  private _listenersCount = 0;
  private _threshold: number;
  private _abortSignal?: Mitto.Abort;
  private _abortSource?: Mitto.Abort;
  private _$close?: Mitto<void>;
  private _$mitto?: Mitto<VALUE>;
  constructor(private options?: Mitto.Options<VALUE>) {
    const { $source, $signal, threshold } = options ?? {};
    this._threshold = threshold ?? 5;

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
    for (let i = 0; i < this._listeners.length; i++) {
      this._listeners[i][0](value);
    }
    // this._$mitto?.push(value)
  }

  listen(listener: Mitto.Listener<VALUE>, $signal?: Mitto<any>): Mitto.Abort {
    if (this._listeners.length > this._threshold) {
      if (!this._$mitto) {
        this._threshold++;
        this._$mitto ??= new Mitto<VALUE>({
          $source: this,
          $signal,
          listenerRemoved: (self) => {
            if (self.listenersCount === 0) {
              this._$mitto?.close();
              this._$mitto = undefined;
            }
          },
        });
        this._threshold--;
      }
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
bench();

function test() {
  const $source = new Mitto<number>();
  const $mitto = new Mitto({ $source, threshold: 2 });

  $mitto.listen((v) => console.log("c1", v));
  $mitto.listen((v) => console.log("c2", v));
  $mitto.listen((v) => console.log("c3", v));
  $mitto.listen((v) => console.log("c4", v));

  $source.push(1);
  $source.push(2);
  $source.push(3);
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
