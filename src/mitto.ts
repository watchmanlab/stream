import { Queue } from "./queue.ts";
import type { Transformer } from "./transformer.ts";

export class Mitto<VALUE = void, NAME extends string = Mitto.Name> {
  readonly name: NAME;
  private _listeners = new Set<Mitto.Listener<VALUE>>();
  private _listenerAdded?: Mitto<Mitto.Listener<VALUE>, `${NAME}ListenerAdded`>;
  private _listenerRemoved?: Mitto<Mitto.Listener<VALUE>, `${NAME}ListenerRemoved`>;
  private _aborted?: Mitto<void, `${NAME}Aborted`>;

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

  get listenersCount() {
    return this._listeners.size;
  }
  get listenerAdded() {
    if (!this._listenerAdded) this._listenerAdded = new Mitto({ name: `${this.name}ListenerAdded` });
    return this._listenerAdded;
  }
  get listenerRemoved() {
    if (!this._listenerRemoved) this._listenerRemoved = new Mitto({ name: `${this.name}ListenerRemoved` });
    return this._listenerRemoved;
  }
  get aborted() {
    if (!this._aborted) this._aborted = new Mitto({ name: `${this.name}Aborted` });
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

    this._listeners.clear();

    return this;
  }
  emit(...values: [value: VALUE, ...values: VALUE[]]): this {
    return this.emitBatch(values.length ? values : ([undefined] as VALUE[]));
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
  export const EMPTY = Symbol.for("EMPTY");
  export type Empty = typeof EMPTY;
}
