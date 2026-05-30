import { Queue } from "./queue.ts";
import type { Transformer } from "./transformer.ts";

export class Mitto<VALUE = void, NAME extends string = Mitto.Name> {
  readonly name: NAME;
  private _listeners = new Set<Mitto.Listener<VALUE>>();
  private _listenerAdded?: Mitto<Mitto.Listener<VALUE>>;
  private _listenerRemoved?: Mitto<Mitto.Listener<VALUE>>;
  private _aborted?: Mitto<void>;

  constructor(private options = {} as Mitto.Options<VALUE, NAME>) {
    this.name = options.name ?? ("root" as NAME);
    if (options.scoop) {
      if (options.scoop instanceof Mitto) {
        options.scoop.aborted.next(() => this.abort());
      } else if (options.scoop.any) {
        let signals: Mitto[] = [];
        new Set(options.scoop.any).forEach((other) => {
          signals.push(
            other.aborted.next(() => {
              this.abort();
              signals.forEach((signal) => signal.emit());
              signals.length = 0;
            }),
          );
        });
      } else {
        const scoops = new Set(options.scoop.all);
        let count = scoops.size;
        scoops.forEach((other) => other.aborted.next(() => !count-- && this.abort()));
      }
    }
    if (options?.source) {
      const { listenerAdded, listenerRemoved, aborted } = options;

      let abort: () => void;
      this.options.listenerAdded = (fn) => {
        if (this.listenersCount === 1) abort = options.source!();

        listenerAdded?.(fn);
      };
      this.options.listenerRemoved = (fn) => {
        if (this.listenersCount === 0) abort();

        listenerRemoved?.(fn);
      };
      this.options.aborted = () => {
        abort();
        aborted?.();
      };
    }
  }
  static create<VALUE, PROPS extends Record<string, any>>() {}
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
  abort(): this {
    this._listeners.clear();
    this._aborted?.emit();
    this._aborted?.abort();
    this.options?.aborted?.();
    this.options.aborted =
      this.options.emited =
      this.options.listenerAdded =
      this.options.listenerRemoved =
      this.options.source =
      this._listenerAdded =
      this._listenerRemoved =
      this._aborted =
        undefined;

    return this;
  }
  emit(...values: [value: VALUE, ...values: VALUE[]]): this {
    return this.emitBatch(values);
  }
  emitBatch(values: VALUE[]): this {
    for (const value of values) {
      this.options?.emited?.(value);
      for (const fn of this._listeners) {
        fn(value);
      }
    }
    return this;
  }
  self(fn: (mitto: this) => void): this {
    fn(this);
    return this;
  }
  listen(fn: Mitto.Listener<VALUE> = () => {}, options?: Mitto.ListenOptions): Mitto<void> {
    const abortSignal = new Mitto({
      emited: () => {
        abortSignal.abort();
        if (!this._listeners.delete(fn)) return;
        this.options?.listenerRemoved?.(fn);
        this._listenerRemoved?.emit(fn);
      },
    });
    if (this._listeners.has(fn)) return abortSignal;

    this._listeners.add(fn);
    this.options?.listenerAdded?.(fn);
    this._listenerAdded?.emit(fn);

    options?.abortSignal?.next(() => abortSignal.abort(), { abortSignal });
    return abortSignal;
  }
  next(fn: Mitto.Listener<VALUE>, options?: Mitto.ListenOptions): Mitto<void> {
    const stopSignal = this.listen((value) => {
      fn?.(value);
      stopSignal.emit();
    }, options);

    return stopSignal;
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

  scoop(other: Mitto.AnyMitto) {
    other.aborted.next(() => this.abort());
  }
  pipe<OUTPUT_NAME extends string, OUTPUT extends Transformer<this, any, OUTPUT_NAME> | this>(
    transform: Mitto.Transform<this, OUTPUT_NAME, OUTPUT>,
  ): OUTPUT;
  pipe<OUTPUT_NAME extends string, OUTPUT extends Transformer<this, any, OUTPUT_NAME> | this>(
    name: OUTPUT_NAME,
    transform: Mitto.Transform<this, OUTPUT_NAME, OUTPUT>,
  ): OUTPUT;
  pipe<OUTPUT_NAME extends string, OUTPUT extends Transformer<this, any, OUTPUT_NAME> | this>(
    nameOrTransform: OUTPUT_NAME | Mitto.Transform<this, OUTPUT_NAME, OUTPUT>,
    transform?: Mitto.Transform<this, OUTPUT_NAME, OUTPUT>,
  ): OUTPUT {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }
}

export namespace Mitto {
  export const NAME = "root";
  export type Name = typeof NAME;
  export type Scoop =
    | Mitto.AnyMitto
    | { any: [other: Mitto.AnyMitto, ...others: Mitto.AnyMitto[]]; all?: never }
    | { all: [other: Mitto.AnyMitto, ...others: Mitto.AnyMitto[]]; any?: never };
  export type Options<VALUE, NAME extends string> = {
    name?: NAME;
    scoop?: Scoop;
    source?: () => () => void;
    emited?: (value: VALUE) => void;
    aborted?: () => void;
    listenerAdded?: (fn: Listener<VALUE>) => void;
    listenerRemoved?: (fn: Listener<VALUE>) => void;
  };
  export type AnyMitto = Mitto<any, any>;
  export type Listener<VALUE> = (value: VALUE) => void;
  export type ListenOptions = { abortSignal?: AnyMitto };

  export type FixedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
    ? ARR
    : FixedArray<VALUE, SIZE, [...ARR, VALUE]>;

  export type ExtractName<T> = T extends { [k in "name"]: any } ? T["name"] : never;

  export type ExtractValue<T extends AnyMitto | Transformer.AnyTransformer> =
    T extends Mitto<infer VALUE, any>
      ? VALUE
      : Transformer.ExtractValue<T> extends never
        ? never
        : Transformer.ExtractValue<T>;

  export type Transform<
    INPUT extends AnyMitto,
    OUTPUT_NAME extends string,
    OUTPUT extends Transformer<INPUT, any, OUTPUT_NAME> | INPUT,
  > = (inputStream: INPUT, name?: OUTPUT_NAME) => OUTPUT;
  export const EMPTY = Symbol.for("EMTY");
  export type Empty = typeof EMPTY;
}
