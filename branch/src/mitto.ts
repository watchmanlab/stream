import { Queue } from "./queue.ts";
import type { Transformer } from "./transformer.ts";

export class Mitto<VALUE = void, NAME extends string = Mitto.Name> {
  readonly name: NAME;
  private _options: Mitto.Options<VALUE, NAME>;
  private _listeners = new Set<Mitto.Listener<VALUE>>();
  private _listenerAdded?: Mitto<Mitto.Listener<VALUE>, `${NAME}ListenerAdded`>;
  private _listenerRemoved?: Mitto<Mitto.Listener<VALUE>, `${NAME}ListenerRemoved`>;
  private _aborted?: Mitto<void, `${NAME}Aborted`>;

  constructor(options?: Mitto.Options<VALUE, NAME>) {
    this._options = { ...options };

    this.name = this._options.name ?? ("root" as NAME);

    if (this._options.scope) {
      if (this._options.scope instanceof Mitto) {
        this._options.scope.aborted.next(() => this.abort());
      } else if (this._options.scope.any) {
        let signals: Mitto[] = [];
        new Set(this._options.scope.any).forEach((other) => {
          signals.push(
            other.aborted.next(() => {
              this.abort();
              signals.forEach((signal) => signal.emit());
              signals.length = 0;
            }),
          );
        });
      } else {
        const scopes = new Set(this._options.scope.all);
        let count = scopes.size;
        scopes.forEach((other) => other.aborted.next(() => !count-- && this.abort()));
      }
    }
    if (this._options?.source) {
      const { listenerAdded, listenerRemoved, aborted } = this._options;

      let signal: Mitto | undefined;
      let cleanup: (...args: any) => void | undefined;
      let stop = false;
      let iterator: Iterator<VALUE> | AsyncIterator<VALUE> | undefined;
      let resolver: (() => void) | undefined;
      this._options.listenerAdded = (fn) => {
        if (this.listenersCount === 1) {
          const source = typeof this._options.source === "function" ? this._options.source() : this._options.source!;
          if (typeof source === "function") {
            cleanup = source;
          } else if (source instanceof Mitto) {
            signal = source.listen((value) => this.emit(value));
          } else {
            resolver?.();
            stop = false;
            if (!iterator) {
              iterator = (source as any)[Symbol.iterator]?.() ?? (source as any)[Symbol.asyncIterator]?.() ?? source;
              (async () => {
                let next = await iterator!.next();

                while (!next.done) {
                  if (stop) await new Promise<void>((r) => (resolver = r));
                  this.emit(next.value);
                  next = await iterator!.next();
                }
                this.abort();
              })();
            }
          }
        }
        listenerAdded?.(fn);
      };
      this._options.listenerRemoved = (fn) => {
        if (this.listenersCount === 0) {
          signal?.emit();
          cleanup?.();
          stop = true;
        }

        listenerRemoved?.(fn);
      };
      this._options.aborted = () => {
        signal?.emit();
        cleanup?.();
        stop = true;
        iterator?.return?.();
        resolver?.();
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
    this._options?.aborted?.();
    this._options.aborted =
      this._options.emited =
      this._options.listenerAdded =
      this._options.listenerRemoved =
      this._options.source =
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
      this._options?.emited?.(value);
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
        this._options?.listenerRemoved?.(fn);
        this._listenerRemoved?.emit(fn);
      },
    });
    if (this._listeners.has(fn)) return abortSignal;

    this._listeners.add(fn);
    this._options?.listenerAdded?.(fn);
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

  protected then<TResult2 = never>(
    onfulfilled?: ((value: VALUE) => VALUE | PromiseLike<VALUE>) | undefined | null,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | undefined | null,
  ): PromiseLike<VALUE | TResult2> {
    return new Promise<VALUE>((resolve, reject) => {
      this.next(resolve);
      this.aborted.next(reject);
    })
      .then(onfulfilled, onrejected)
      .catch();
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

  export type Source<VALUE = never> =
    | Mitto<VALUE, any>
    | Iterable<VALUE>
    | AsyncIterable<VALUE>
    | Iterator<VALUE>
    | AsyncIterator<VALUE>
    | (() =>
        | Mitto<VALUE, any>
        | Iterable<VALUE>
        | AsyncIterable<VALUE>
        | Iterator<VALUE>
        | AsyncIterator<VALUE>
        | Generator<VALUE>
        | AsyncGenerator<VALUE>
        | (() => void));
  export type Options<VALUE, NAME extends string> = {
    name?: NAME;
    scope?: Scoop;
    source?: Source<VALUE>;
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
