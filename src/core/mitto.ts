import { Queue } from "./queue";

export class Mitto<VALUE = void> {
  private _listeners = new Set<Mitto.Listener<VALUE>>();
  private _listenerAdded?: Mitto<Mitto.Listener<VALUE>>;
  private _listenerRemoved?: Mitto<Mitto.Listener<VALUE>>;
  private _aborted?: Mitto<void>;

  constructor(private options = {} as Mitto.Options<VALUE, Mitto<VALUE>>) {
    if (options.scoop) {
      if (options.scoop instanceof Mitto) {
        options.scoop.aborted.next(() => this.abort());
      } else if (options.scoop.any) {
        options.scoop.any.forEach((other) => other.aborted.next(() => this.abort()));
      } else {
        let count = options.scoop.all.length;
        options.scoop.all.forEach((other) => other.aborted.next(() => !count-- && this.abort()));
      }
    }

    if (options?.source) {
      const { listenerAdded, listenerRemoved, aborted } = options;

      let abort: () => void;
      this.options.listenerAdded = (fn, self) => {
        if (this.listenersCount === 1) abort = options.source!(this);

        listenerAdded?.(fn, self);
      };
      this.options.listenerRemoved = (fn, self) => {
        if (this.listenersCount === 0) abort();

        listenerRemoved?.(fn, self);
      };
      this.options.aborted = (self) => {
        abort();
        aborted?.(self);
      };
    }
  }

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
    this.options.aborted =
      this.options.emited =
      this.options.listenerAdded =
      this.options.listenerRemoved =
      this.options.source =
      this._listenerAdded =
      this._listenerRemoved =
      this._aborted =
        undefined;
  }
  emit(...values: [value: VALUE, ...values: VALUE[]]) {
    for (const value of values) {
      this.options?.emited?.(value, this);
      for (const fn of this._listeners) {
        fn(value);
      }
    }
  }
  listen(fn: Mitto.Listener<VALUE> = () => {}, options?: Mitto.ListenOptions): Mitto<void> {
    this._listeners.add(fn);
    this.options?.listenerAdded?.(fn, this);
    this._listenerAdded?.emit(fn);

    const abortSignal = new Mitto({
      emited: () => {
        abortSignal.abort();
        if (!this._listeners.delete(fn)) return;
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
    return new Mitto<MAPPED>({
      scoop: this,
      source: (self) => this.listen((value) => self.emit(fn(value))).emit.bind(self),
    });
  }
  filter<FILTERED extends VALUE = VALUE>(predicate: (value: VALUE) => value is FILTERED): Mitto<FILTERED>;
  filter(predicate: (value: VALUE) => boolean): Mitto<VALUE>;
  filter<FILTERED extends VALUE = VALUE>(predicate: (value: VALUE) => value is FILTERED) {
    return new Mitto<FILTERED>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          const result = predicate(value);
          if (result) self.emit(value);
        }).emit.bind(self),
    });
  }
  merge<MITTOS extends [mitto: Mitto<any>, ...mittos: Mitto<any>[]]>(...mittos: MITTOS) {
    return new Mitto<VALUE | Mitto.ExtractValue<MITTOS[number]>>({
      scoop: this,
      source: (self) => {
        const signals = [this, ...mittos].map((m) => m.listen((value) => self.emit(value)));
        return () => signals.forEach((abort) => abort.emit());
      },
    });
  }
  flat<DEPTH extends number = 0>(depth = 0 as DEPTH) {
    return new Mitto<FlatArray<VALUE, DEPTH>>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          if (Array.isArray(value)) {
            const flatten = depth === 0 ? value : value.flat(depth);
            for (let i = 0, length = flatten.length; i < length; i++) {
              self.emit(flatten[i]);
            }
          } else {
            self.emit(value as never);
          }
        }).emit.bind(self),
    });
  }
  group<SIZE extends number = 2>(size = 2 as SIZE) {
    const buffer = new Array();

    return new Mitto<Mitto.FixedArray<VALUE, SIZE>>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          buffer.push(value);

          if (buffer.length === size) {
            const out = [...buffer];
            buffer.length = 0;
            self.emit(out as never);
          }
        }).emit.bind(self),
      aborted: () => (buffer.length = 0),
    });
  }
  debounce(ms: number) {
    let timer: any = null;
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) => {
        const signal = this.listen((value) => {
          clearTimeout(timer);
          timer = setTimeout(() => self.emit(value), ms);
        });

        return () => {
          signal.emit();
          clearTimeout(timer);
        };
      },
      aborted: () => clearTimeout(timer),
    });
  }
  throttle(ms: number) {
    let inThrottle = false;
    let timer: any = null;

    return new Mitto<VALUE>({
      scoop: this,
      source: (self) => {
        const signal = this.listen((value) => {
          if (inThrottle) return;

          self.emit(value);
          inThrottle = true;

          timer = setTimeout(() => {
            inThrottle = false;
          }, ms);
        });

        return () => {
          signal.emit();
          clearTimeout(timer);
        };
      },
      aborted: () => clearTimeout(timer),
    });
  }
  take(count: number) {
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          self.emit(value);
          if (--count === 0) self.abort();
        }).emit.bind(self),
    });
  }
  takeWhile(fn: (value: VALUE) => boolean) {
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          if (!fn(value)) {
            self.abort();
            return;
          }

          self.emit(value);
        }).emit.bind(self),
    });
  }
  takeUntil(fn: (value: VALUE) => boolean) {
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          if (fn(value)) {
            self.abort();
            return;
          }

          self.emit(value);
        }).emit.bind(self),
    });
  }
  skip(count: number) {
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          if (--count > 0) return;
          self.emit(value);
        }).emit.bind(self),
    });
  }
  skipWhile(fn: (value: VALUE) => boolean) {
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          if (fn(value)) return;

          self.emit(value);
        }).emit.bind(self),
    });
  }
  skipUntil(fn: (value: VALUE) => boolean) {
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          if (!fn(value)) return;

          self.emit(value);
        }).emit.bind(self),
    });
  }
  signal() {
    return new Mitto({
      scoop: this,
      source: (self) =>
        this.next(() => {
          self.emit();
          self.abort();
        }).emit.bind(self),
    });
  }
  distinct() {
    let last: VALUE;
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          if (value !== last) {
            last = value;
            self.emit(value);
          }
        }).emit.bind(self),
    });
  }
  distinctBy<KEY>(fn: (value: VALUE) => KEY) {
    let last: KEY;
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          const key = fn(value);
          if (key !== last) {
            last = key;
            self.emit(value);
          }
        }).emit.bind(self),
    });
  }
  scan<ACC>(seed: ACC, fn: (acc: ACC, value: VALUE) => ACC) {
    let acc = seed;
    return new Mitto<ACC>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          acc = fn(acc, value);
          self.emit(acc);
        }).emit.bind(self),
    });
  }
  delay(ms: number) {
    let timer: any = null;
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          timer = setTimeout(() => self.emit(value), ms);
        }).emit.bind(self),
      aborted: () => clearTimeout(timer),
    });
  }
  auditTime(ms: number) {
    let timer: any = null;
    let latest: VALUE;
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) => {
        const signal = this.listen((value) => {
          latest = value;
          if (!timer) {
            timer = setTimeout(() => {
              self.emit(latest);
              timer = null;
            }, ms);
          }
        });
        return () => {
          signal.emit();
          clearTimeout(timer);
        };
      },
      aborted: () => clearTimeout(timer),
    });
  }
  combineLatest<OTHERS extends readonly [other: Mitto.AnyMitto, ...others: Mitto.AnyMitto[]]>(...others: OTHERS) {
    const mittos = [this, ...others] as [other: Mitto.AnyMitto, ...others: Mitto.AnyMitto[]];

    const EMPTY = Symbol.for("EMPTY");

    const values = new Array(others.length + 1).fill(EMPTY);

    return new Mitto<[curr: VALUE, ...{ [K in keyof OTHERS]: Mitto.ExtractValue<OTHERS[K]> }]>({
      scoop: { any: mittos },
      source: (self) => {
        const signals = mittos.map((mitto, index) =>
          mitto.listen((value) => {
            values[index] = value;

            if (values.every((v) => v !== EMPTY)) self.emit([...values] as never);
          }),
        );

        return () => signals.forEach((signal) => signal.emit());
      },

      aborted: () => {
        mittos.length = 0;
        values.length = 0;
      },
    });
  }
  withLatestFrom<
    OTHER extends Mitto.AnyMitto,
    OTHER_VALUE extends Mitto.ExtractValue<OTHER> = Mitto.ExtractValue<OTHER>,
  >(other: OTHER) {
    let lastOther: OTHER_VALUE | Mitto.Empty;

    return new Mitto<[curr: VALUE, other: OTHER_VALUE]>({
      scoop: { any: [this, other] },
      source: (self) => {
        const s1 = other.listen((v) => {
          lastOther = v;
        });
        const s2 = this.listen((v) => {
          if (lastOther !== Mitto.EMPTY) self.emit([v, lastOther]);
        });
        return () => {
          s1.emit();
          s2.emit();
        };
      },
    });
  }
  tap(fn: (value: VALUE) => void) {
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          fn(value);
          self.emit(value);
        }).emit.bind(self),
    });
  }
  startWith(...values: [value: VALUE, ...values: VALUE[]]) {
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) => {
        for (const value of values) self.emit(value);
        return this.listen((v) => self.emit(v)).emit.bind(self);
      },
    });
  }
  pairwise() {
    let prev: VALUE | Mitto.Empty;
    return new Mitto<[prev: VALUE, curr: VALUE]>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          if (prev !== Mitto.EMPTY) self.emit([prev, value]);
          prev = value;
        }).emit.bind(self),
    });
  }
  first(predicate?: (value: VALUE) => boolean) {
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          if (!predicate || predicate(value)) {
            self.emit(value);
            self.abort();
          }
        }).emit.bind(self),
    });
  }
  last() {
    let latest: VALUE;
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) => {
        const signal = this.listen((value) => (latest = value));
        this.aborted.next(() => {
          self.emit(latest);
          self.abort();
        });
        return signal.emit.bind(signal);
      },
    });
  }
  catchError(fn: (error: any) => VALUE) {
    return new Mitto<VALUE>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          try {
            self.emit(value);
          } catch (error) {
            self.emit(fn(error));
          }
        }).emit.bind(self),
    });
  }
  sample(notifier: Mitto<any>) {
    let latest: VALUE | Mitto.Empty;

    return new Mitto<VALUE>({
      scoop: { any: [this, notifier] },
      source: (self) => {
        const s1 = this.listen((v) => {
          latest = v;
        });
        const s2 = notifier.listen(() => {
          if (latest !== Mitto.EMPTY) self.emit(latest);
        });
        return () => {
          s1.emit();
          s2.emit();
        };
      },
    });
  }
  bufferTime(ms: number) {
    const buffer: VALUE[] = [];
    let timer: any = null;

    return new Mitto<VALUE[]>({
      scoop: this,
      source: (self) => {
        const signal = this.listen((value) => {
          buffer.push(value);

          if (!timer) {
            timer = setTimeout(() => {
              if (buffer.length > 0) {
                self.emit([...buffer]);
                buffer.length = 0;
              }
              timer = null;
            }, ms);
          }
        });

        return () => {
          signal.emit();
          clearTimeout(timer);
        };
      },
      aborted: () => {
        clearTimeout(timer);
        buffer.length = 0;
      },
    });
  }
  bufferCount<SIZE extends number>(size: SIZE, startBufferEvery = size) {
    const buffers = new Queue<VALUE[]>();
    let count = 0;

    return new Mitto<Mitto.FixedArray<SIZE>>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          if (count % startBufferEvery === 0) {
            buffers.enqueue([]);
          }

          for (const buffer of buffers) {
            buffer.push(value);
            if (buffer.length === size) {
              self.emit([...buffer] as never);
              buffers.dequeue();
            }
          }

          count++;
        }).emit.bind(self),
      aborted: () => {
        buffers.clear();
        count = 0;
      },
    });
  }
  windowTime(ms: number) {
    let window: Mitto<VALUE> | null = null;
    let timer: any = null;

    return new Mitto<Mitto<VALUE>>({
      scoop: this,
      source: (self) => {
        const signal = this.listen((value) => {
          if (!window) {
            window = new Mitto<VALUE>();
            self.emit(window);

            timer = setTimeout(() => {
              window?.abort();
              window = null;
              timer = null;
            }, ms);
          }

          window.emit(value);
        });

        return () => {
          signal.emit();
          clearTimeout(timer);
          window?.abort();
        };
      },
      aborted: () => {
        clearTimeout(timer);
        window?.abort();
      },
    });
  }
  windowCount(size: number, startWindowEvery = size) {
    const windows: Mitto<VALUE>[] = [];
    let count = 0;

    return new Mitto<Mitto<VALUE>>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          if (count % startWindowEvery === 0) {
            const window = new Mitto<VALUE>();
            windows.push(window);
            self.emit(window);
          }

          for (const window of windows) {
            window.emit(value);
          }

          windows.forEach((window, i) => {
            if (++count >= size) {
              window.abort();
              windows.splice(i, 1);
            }
          });

          count++;
        }).emit.bind(self),
      aborted: () => {
        windows.forEach((w) => w.abort());
        windows.length = 0;
        count = 0;
      },
    });
  }
  bufferWhen(notifier: Mitto.AnyMitto) {
    const buffer: VALUE[] = [];

    return new Mitto<VALUE[]>({
      scoop: { any: [this, notifier] },
      source: (self) => {
        const s1 = this.listen((value) => buffer.push(value));
        const s2 = notifier.listen(() => {
          if (buffer.length > 0) {
            self.emit([...buffer]);
            buffer.length = 0;
          }
        });

        return () => {
          s1.emit();
          s2.emit();
        };
      },
      aborted: () => (buffer.length = 0),
    });
  }
  bufferToggle(opening: Mitto.AnyMitto, closingSelector: () => Mitto.AnyMitto) {
    const buffers = new Map<number, VALUE[]>();
    let id = 0;

    return new Mitto<VALUE[]>({
      scoop: { any: [this, opening] },
      source: (self) => {
        const s1 = this.listen((value) => {
          for (const buffer of buffers.values()) {
            buffer.push(value);
          }
        });

        const s2 = opening.listen(() => {
          const bufferId = id++;
          const buffer: VALUE[] = [];
          buffers.set(bufferId, buffer);

          const closing = closingSelector();
          closing.next(() => {
            buffers.delete(bufferId);
            self.emit([...buffer]);
            closing.abort();
          });
        });

        return () => {
          s1.emit();
          s2.emit();
        };
      },
      aborted: () => buffers.clear(),
    });
  }
  slidingWindow(size: number) {
    const buffer = new Queue<VALUE>();

    return new Mitto<VALUE[]>({
      scoop: this,
      source: (self) =>
        this.listen((value) => {
          buffer.enqueue(value);
          if (buffer.size > size) buffer.dequeue();
          if (buffer.size === size) self.emit([...buffer]);
        }).emit.bind(self),
      aborted: () => buffer.clear(),
    });
  }
}

export namespace Mitto {
  export type Options<VALUE, SELF extends Mitto<VALUE>> = {
    scoop?:
      | Mitto.AnyMitto
      | { any: [other: Mitto.AnyMitto, ...others: Mitto.AnyMitto[]]; all?: never }
      | { all: [other: Mitto.AnyMitto, ...others: Mitto.AnyMitto[]]; any?: never };
    source?: (self: SELF) => () => void;
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

  export const EMPTY = Symbol.for("EMTY");
  export type Empty = typeof EMPTY;
}

const m1 = new Mitto<number>();

m1.map((v) => {
  console.log("map", v);
  return v.toLocaleString();
})
  .filter((v) => {
    console.log("filrer", v);
    return v.length > 0;
  })
  .listen(console.log);

m1.emit(1);
m1.emit(2);
