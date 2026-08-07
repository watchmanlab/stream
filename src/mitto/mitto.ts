export class Mitto<VALUE> {
  private _listener: Mitto.Listener<VALUE> | Mitto.EmptyFunction = Mitto.EMPTY_FUNCTION;
  private _$next?: Mitto<VALUE>;
  private _$prev?: Mitto<VALUE>;
  private _$tail: Mitto<VALUE> = this;
  private _abortSignal?: Mitto.Abort;
  private _abortSource?: Mitto.Abort;
  private _$clear?: Mitto<void>;
  private _listenersCount = 0;

  // Track if this specific node instance has been replaced/merged
  private _$replacedBy?: Mitto<VALUE>;

  constructor(private options?: Mitto.Options<VALUE>) {
    const { $source, $signal } = options ?? {};
    if ($signal) this._abortSignal = $signal.listen(() => this.clear());
    if ($source) {
      this._abortSource = $source.listen((v) => this.push(v));
      $source._$clear ??= new Mitto();
      $source._$clear.listen(() => this.clear());
    }
  }

  get listenersCount() {
    return this._listenersCount;
  }

  get $clear() {
    this._$clear ??= new Mitto();
    return new Mitto({ $source: this._$clear });
  }

  push(value: VALUE) {
    this._listener(value);

    let $next: Mitto<VALUE> | undefined = this._$next;
    while ($next) {
      if ($next._listener !== Mitto.EMPTY_FUNCTION) {
        $next._listener(value);
      }
      $next = $next._$next;
    }
  }

  listen(listener: Mitto.Listener<VALUE>): Mitto.Abort {
    this._listenersCount++;

    // If the root node is empty, use it directly (No indirection, no allocation!)
    if (this._listener === Mitto.EMPTY_FUNCTION && this._$next === undefined) {
      this._listener = listener;
      this.options?.firstListenerAdded?.(this, listener);
      this.options?.listenerAdded?.(this, listener);

      let $current = this;
      return () => {
        if ($current._listener === Mitto.EMPTY_FUNCTION) return;
        this._listenersCount--;

        // Resolve tracking if this node was merged into later
        const target = $current._$replacedBy ?? $current;
        // while (target._$replacedBy) {
        //   target = target._$replacedBy;
        // }

        if (target === this) {
          if (this._$next) {
            const nextNode = this._$next;
            this._listener = nextNode._listener;
            this._$next = nextNode._$next;
            if (this._$next) this._$next._$prev = this;
            if (this._$tail === nextNode) this._$tail = this;
            nextNode._$replacedBy = this; // Redirect closures
          } else {
            this._listener = Mitto.EMPTY_FUNCTION;
            this._$tail = this;
          }
        } else {
          // It became a middle/tail node due to shifts
          this._removeNode(target);
        }
      };
    }

    // Otherwise, append a new Mitto node
    const node = new Mitto<VALUE>();
    node._listener = listener;

    const $prevTail = this._$tail;
    this._$tail = node;
    node._$prev = $prevTail;
    $prevTail._$next = node;

    this.options?.listenerAdded?.(this, listener);

    let $current = node;
    return () => {
      this._listenersCount--;

      // Follow the replacement chain if this node's data was shifted up
      let target = $current;
      while (target._$replacedBy) {
        target = target._$replacedBy;
      }

      this._removeNode(target);
    };
  }

  private _removeNode(target: Mitto<VALUE>) {
    if (target === this) {
      if (this._$next) {
        const nextNode = this._$next;
        this._listener = nextNode._listener;
        this._$next = nextNode._$next;
        if (this._$next) this._$next._$prev = this;
        if (this._$tail === nextNode) this._$tail = this;
        nextNode._$replacedBy = this;
      } else {
        this._listener = Mitto.EMPTY_FUNCTION;
        this._$tail = this;
      }
    } else if (target === this._$tail) {
      this._$tail = target._$prev ?? this;
      this._$tail._$next = undefined;
    } else {
      if (target._$prev) target._$prev._$next = target._$next;
      if (target._$next) target._$next._$prev = target._$prev;
    }
  }

  clear() {
    this._listener = Mitto.EMPTY_FUNCTION;
    this._$next = undefined;
    this._$prev = undefined;
    this._$tail = this;
    this._listenersCount = 0;
    this._abortSignal?.();
    this._abortSource?.();
    this.options?.clear?.(this);
    this._$clear?.push();
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
    firstListenerAdded?: (self: Mitto<VALUE>, listener: Listener<VALUE>) => void;
    lastListenerRemoved?: (self: Mitto<VALUE>, listener: Listener<VALUE>) => void;
  };
  export const EMPTY_FUNCTION = () => {};
  export type EmptyFunction = typeof EMPTY_FUNCTION;
}

function test() {
  const $mitto = new Mitto();

  const abort1 = $mitto.listen((v) => console.log("c1", v));
  const abort2 = $mitto.listen((v) => console.log("c2", v));
  const abort3 = $mitto.listen((v) => console.log("c3", v));

  // abort1();
  // abort2();
  // abort3();

  $mitto.push(1);

  $mitto.push(2);
}

// test();

export const map = <T, R>(fn: (val: T) => R) => {
  return (source: Mitto<T>): Mitto<R> => {
    let abort: Mitto.Abort;

    const destination = new Mitto<R>({
      firstListenerAdded(self, listener) {
        abort = source.listen((val) => destination.push(fn(val)));
      },
      lastListenerRemoved(self, listener) {
        abort();
      },
    });

    return destination;
  };
};

export const filter = <T>(predicate: (val: T) => boolean) => {
  return (source: Mitto<T>): Mitto<T> => {
    let abort: Mitto.Abort;

    const destination = new Mitto<T>({
      firstListenerAdded(self, listener) {
        abort = source.listen((val) => {
          if (predicate(val)) destination.push(val);
        });
      },
      lastListenerRemoved(self, listener) {
        abort();
      },
    });
    return destination;
  };
};
export function scan<INPUT, OUTPUT>(accumulator: (acc: OUTPUT, value: INPUT) => OUTPUT, seed: OUTPUT) {
  return (source: Mitto<INPUT>): Mitto<OUTPUT> => {
    const destination = new Mitto<OUTPUT>();
    let state = seed;

    source.listen((value) => {
      state = accumulator(state, value);
      destination.push(state);
    });

    return destination;
  };
}
function bench() {
  const MAX = 1_000_000;

  const start = performance.now();

  const mitto = new Mitto<number>();

  mitto
    .pipe(map((v) => v + 100))
    .pipe(map((v) => v - 100))
    .pipe(map((v) => v + 100))
    .pipe(map((v) => v - 100))
    .pipe(map((v) => v + 100))
    .pipe(map((v) => v - 100))

    .listen((v) => {
      if (v === MAX) console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
    });

  for (let i = 0; i <= MAX; i++) {
    mitto.push(i);
  }
}
bench(); //1 000 000 50 ms

function churnBench() {
  const TOTAL_EVENTS = 10_000;
  const mitto = new Mitto<number>();

  for (let i = 0; i < 4; i++) {
    mitto.listen((v) => {
      //   if (v === TOTAL_EVENTS - 1) console.log(`c${i}`, `${Math.round(performance.now() - start)}ms`);
    });
  }

  let receivedCount = 0;
  const start = performance.now();

  for (let i = 0; i <= TOTAL_EVENTS; i++) {
    const unsub1 = mitto.listen((v) => {
      if (v === TOTAL_EVENTS) console.log("c101", `${Math.round(performance.now() - start)}ms`);
      receivedCount++;
    });
    const unsub2 = mitto.listen((v) => {
      if (v === TOTAL_EVENTS) console.log("c102", `${Math.round(performance.now() - start)}ms`);
      receivedCount++;
    });

    mitto.push(i);

    unsub1();
    unsub2();
  }

  const duration = Math.round(performance.now() - start);
  console.log(`Processed ${TOTAL_EVENTS.toLocaleString()} events with massive churn.`);
  console.log(`Total callbacks triggered: ${receivedCount.toLocaleString()}`);
  console.log(`Time taken: ${duration} ms`);
}
// churnBench();
