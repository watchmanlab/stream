import { Queue } from "./queue";

export class Mitto<VALUE = void> {
  private _listeners = new Set<Mitto.Listener<VALUE>>();
  private _listenerAdded?: Mitto<Mitto.Listener<VALUE>>;
  private _listenerRemoved?: Mitto<Mitto.Listener<VALUE>>;
  private _aborted?: Mitto<void>;

  constructor(private options?: Mitto.Options<VALUE, Mitto<VALUE>>) {}

  get listenersCount() {
    return this._listeners.size;
  }
  get listenerAdded() {
    if (!this._listenerAdded) this._listenerAdded = new Mitto();
    return this._listenerAdded;
  }
  get listenerRemoved() {
    if (!this._listenerRemoved) this._listenerRemoved = new Mitto();
    return this._listenerRemoved;
  }
  get aborted() {
    if (!this._aborted) this._aborted = new Mitto();
    return this._aborted;
  }
  async *[Symbol.asyncIterator]() {
    const queue = new Queue<VALUE>();
    let resolve: ((value: VALUE) => void) | null = null;

    const abort = this.listen((value) => {
      if (resolve) {
        resolve(value);
        resolve = null;
      } else {
        queue.enqueue(value);
      }
    });
    try {
      while (true) {
        const value = queue.dequeue();
        if (value !== Queue.EMPTY) {
          yield value;
        } else {
          yield await new Promise<VALUE>((r) => (resolve = r));
        }
      }
    } finally {
      abort.emit();
      queue.clear();
      resolve = null;
    }
  }
  [Symbol.dispose]() {
    this.abort();
  }
  abort() {
    this._listeners.clear();
    this._aborted?.emit();
    this._aborted?.abort();
    this.options?.aborted?.(this);
    this.options = this._listenerAdded = this._listenerRemoved = this._aborted = undefined;
  }
  emit(value: VALUE) {
    this.options?.emited?.(value, this);
    for (const fn of this._listeners) {
      fn(value);
    }
  }
  listen(fn: Mitto.Listener<VALUE>, options?: Mitto.ListenOptions): Mitto<void> {
    this._listeners.add(fn);
    this.options?.listenerAdded?.(fn, this);
    this._listenerAdded?.emit(fn);

    const abortSignal = new Mitto({
      emited: () => {
        abortSignal.abort();
        this._listeners.delete(fn);
        this.options?.listenerRemoved?.(fn, this);
        this._listenerRemoved?.emit(fn);
      },
    });

    options?.abortSignal?.next(() => abortSignal.abort(), { abortSignal });
    return abortSignal;
  }
  next(fn: Mitto.Listener<VALUE>, options?: Mitto.ListenOptions) {
    const stopSignal = this.listen((value) => {
      fn?.(value);
      stopSignal.emit();
    }, options);

    return stopSignal;
  }
  derive() {
    return this.map((v) => v);
  }
  map<MAPPED = VALUE>(fn: (value: VALUE) => MAPPED) {
    let abortSignal: Mitto;
    const mitto = new Mitto<MAPPED>({
      aborted: (self) => {
        abortSignal.emit();
      },
      listenerAdded: (_, self) => {
        if (self.listenersCount > 1) return;
        abortSignal = this.listen((value) => self.emit(fn(value)));
      },
      listenerRemoved: (_, self) => {
        if (self.listenersCount > 0) return;
        abortSignal.emit();
      },
    });

    this.aborted.next(() => {
      mitto.abort();
    });
    return mitto;
  }
  filter<FILTERED extends VALUE = VALUE>(fn: (value: VALUE) => value is FILTERED) {
    const mitto = new Mitto<FILTERED>();

    const abort = this.listen((value) => {
      const result = fn(value);
      if (result) mitto.emit(value);
    });

    this.aborted.next(() => {
      mitto.abort();
      abort.emit();
    });
    return mitto;
  }
  merge<MITTOS extends [mitto: Mitto<any>, ...mittos: Mitto<any>[]]>(...mittos: MITTOS) {
    const mitto = new Mitto<VALUE | Mitto.ExtractValue<MITTOS[number]>>();

    const aborts = [this, ...mittos].map((m) => m.listen((value) => mitto.emit(value)));

    this.aborted.next(() => {
      mitto.abort();
      aborts.forEach((abort) => abort.emit());
    });
    return mitto;
  }
  flat<DEPTH extends number = 0>(depth = 0 as DEPTH) {
    const mitto = new Mitto<FlatArray<VALUE, DEPTH>>();
    const abort = this.listen((value) => {
      if (Array.isArray(value)) {
        const flatten = depth === 0 ? value : value.flat(depth);
        for (let i = 0, length = flatten.length; i < length; i++) {
          mitto.emit(flatten[i]);
        }
      } else {
        mitto.emit(value as never);
      }
    });

    this.aborted.next(() => {
      mitto.abort();
      abort.emit();
    });
    return mitto;
  }
  group<SIZE extends number = 2>(size = 2 as SIZE) {
    const mitto = new Mitto<Mitto.FixedArray<VALUE, SIZE>>();
    const buffer = new Array();
    const abort = this.listen((value) => {
      buffer.push(value);

      if (buffer.length === size) {
        const out = [...buffer];
        buffer.length = 0;
        mitto.emit(out as never);
      }
    });

    this.aborted.next(() => {
      mitto.abort();
      abort.emit();
    });
    return mitto;
  }
  debounce(ms: number) {
    const mitto = new Mitto<VALUE>();
    let timer: any = null;

    const abort = this.listen((value) => {
      clearTimeout(timer);
      timer = setTimeout(() => mitto.emit(value), ms);
    });
    this.aborted.next((value) => {
      mitto.abort();
      clearTimeout(timer);
      abort.emit();
    });
    return mitto;
  }
  throttle(ms: number) {
    const mitto = new Mitto<VALUE>();
    let inThrottle = false;
    let timer: any = null;

    const abort = this.listen((value) => {
      if (inThrottle) return;

      mitto.emit(value);
      inThrottle = true;

      timer = setTimeout(() => {
        inThrottle = false;
      }, ms);
    });

    this.aborted.next(() => {
      mitto.abort();
      clearTimeout(timer);
      abort.emit();
    });
    return mitto;
  }
}

export namespace Mitto {
  export type Options<VALUE, SELF extends Mitto<VALUE>> = {
    emited?: (value: VALUE, self: SELF) => void;
    aborted?: (self: SELF) => void;
    listenerAdded?: (fn: Listener<VALUE>, self: SELF) => void;
    listenerRemoved?: (fn: Listener<VALUE>, self: SELF) => void;
  };
  export type AnyMitto = Mitto<any>;
  export type Listener<VALUE> = (value: VALUE) => void;
  export type ListenOptions = { abortSignal?: AnyMitto };
  export type ExtractValue<T extends Mitto<any>> = T extends Mitto<infer VALUE> ? VALUE : never;
  export type FixedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
    ? ARR
    : FixedArray<VALUE, SIZE, [...ARR, VALUE]>;
}

const m = new Mitto<number[][]>();

m.flat(1).group().listen(console.log);

const grouped = m.flat(1).group();

(async () => {
  for await (const value of grouped) {
    console.log("gen", value);
  }
})();

m.emit([
  [1, 2],
  [3, 4],
]);
