import type { Transformer } from "./transformer";
export class Mitto<VALUE = void, NAME extends string = Mitto.Name> {
  readonly name: NAME;
  private _options?: Mitto.Options<VALUE, NAME>;
  private _listeners: Mitto.Subscription<VALUE>[] = [];
  private _lifecycles?: Mitto.Lifecycles<VALUE>;

  private _emit = (value: VALUE) => {};
  constructor(options?: Mitto.Options<VALUE, NAME>) {
    this._options = options ? { ...options } : undefined;

    this.name = this._options?.name ?? ("root" as NAME);

    if (this._options?.source) {
      const { listenerAdded, listenerRemoved, aborted } = this._options;

      let signal: Mitto | undefined;
      let cleanup: (...args: any) => void | undefined;

      this._options.listenerAdded = (fn) => {
        if (this._listeners.length === 1) {
          const source = typeof this._options?.source === "function" ? this._options.source() : this._options?.source;
          if (typeof source === "function") {
            cleanup = source;
          } else if (source instanceof Mitto) {
            signal = source.listen((value) => this.emit(value));
          }
        }
        listenerAdded?.(fn);
      };
      this._options.listenerRemoved = (fn) => {
        if (this._listeners.length === 0) {
          signal?.emit();
          cleanup?.();
        }

        listenerRemoved?.(fn);
      };
      this._options.aborted = () => {
        signal?.emit();
        cleanup?.();
        aborted?.();
      };
    }
    if (this._options?.scope) {
      if (this._options.scope instanceof Mitto) {
        this._options.scope.get("aborted").listen(() => this.abort());
      } else if (this._options.scope.any) {
        const aborts = new Array<Mitto>();

        new Set(this._options.scope.any).forEach((scope) =>
          aborts.push(
            scope
              .get("aborted")
              .listen(() => (this.abort(), aborts.forEach((abort) => abort.emit()), (aborts.length = 0))),
          ),
        );
      } else {
        const scopes = new Set(this._options.scope.all);
        let count = scopes.size;
        scopes.forEach((scope) => scope.get("aborted").listen(() => !--count && this.abort()));
      }
    }
  }

  private _swapEmit() {
    switch (this._listeners.length) {
      case 0:
        this._emit = this._options?.emitted ? (value: VALUE) => this._options!.emitted!(value) : (value: VALUE) => {};
        break;
      case 1:
        const sub = this._listeners[0]!;
        this._emit = this._options?.emitted
          ? (value: VALUE) => {
              sub.listener(value);
              this._options!.emitted!(value);
            }
          : sub.listener;
        break;
      default:
        this._emit = this._options?.emitted
          ? (value: VALUE) => {
              for (let i = 0, length = this._listeners.length; i < length; i++) {
                this._listeners[i]!.listener(value);
              }
              this._options!.emitted!(value);
            }
          : (value: VALUE) => {
              for (let i = 0, length = this._listeners.length; i < length; i++) {
                this._listeners[i]!.listener(value);
              }
            };
    }
  }

  static once<VALUE>(source: Mitto<VALUE>, listener: Mitto.Listener<VALUE>): Mitto {
    const abort = source.listen((value) => {
      listener(value);
      abort.emit();
    });
    return abort;
  }

  emit(value: VALUE) {
    this._emit(value);
  }
  listen(listener: Mitto.Listener<VALUE>): Mitto {
    const abortSignal = new Mitto<void>({ emitted: abort });
    const sub: Mitto.Subscription<VALUE> = {
      listener,
      index: this._listeners.length,
    };

    this._listeners.push(sub);
    this._options?.listenerAdded?.(listener);
    this._lifecycles?.listenerAdded?.emit(listener);
    this._swapEmit();

    const self = this;

    return abortSignal;

    function abort() {
      abortSignal.abort();
      const idx = sub.index;
      if (idx === -1) return;

      const last = self._listeners.pop()!;

      if (idx < self._listeners.length) (self._listeners[idx] = last).index = idx;

      sub.index = -1;
      self._swapEmit();
      self._options?.listenerRemoved?.(listener);
      self._lifecycles?.listenerRemoved?.emit(listener);
    }
  }
  abort() {
    this._listeners.length = 0;
    this._lifecycles?.listenerAdded?.abort();
    this._lifecycles?.listenerRemoved?.abort();
    this._lifecycles?.aborted?.emit();
    this._lifecycles?.aborted?.abort();
    this._options?.aborted?.();
    this._lifecycles = this._options = undefined;
  }
  [Symbol.dispose]() {
    this.abort();
  }
  get<T extends Mitto.LifecycleName>(lifecycle: T): NonNullable<Mitto.Lifecycles<VALUE>[T]> {
    if (!this._lifecycles) this._lifecycles = {};
    switch (lifecycle) {
      case "aborted":
        if (!this._lifecycles.aborted) this._lifecycles.aborted = new Mitto();
        return new Mitto({ source: this._lifecycles.aborted, scope: this._lifecycles.aborted }) as never;
      case "listenerAdded":
        if (!this._lifecycles.listenerAdded) this._lifecycles.listenerAdded = new Mitto();
        return new Mitto({
          source: this._lifecycles.listenerAdded,
          scope: this._lifecycles.listenerAdded,
        }) as never;
      case "listenerRemoved":
        if (!this._lifecycles.listenerRemoved) this._lifecycles.listenerRemoved = new Mitto();
        return new Mitto({
          source: this._lifecycles.listenerRemoved,
          scope: this._lifecycles.listenerRemoved,
        }) as never;
      case "listenersCount":
        return this._listeners.length as never;
    }
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
  export type AnyMitto = Mitto<any, any>;
  export type Scope =
    | Mitto.AnyMitto
    | { any: [other: Mitto.AnyMitto, ...others: Mitto.AnyMitto[]]; all?: never }
    | { all: [other: Mitto.AnyMitto, ...others: Mitto.AnyMitto[]]; any?: never };

  export type Options<VALUE, NAME extends string> = {
    name?: NAME;
    source?: Mitto<VALUE> | (() => Mitto<VALUE, any> | (() => void));
    scope?: Scope;
    emitted?: (value: VALUE) => void;
    aborted?: () => void;
    listenerAdded?: (listener: Listener<VALUE>) => void;
    listenerRemoved?: (listener: Listener<VALUE>) => void;
  };

  export type Abort = () => void;
  export type Listener<VALUE> = (value: VALUE) => void;
  export type Subscription<VALUE> = { listener: Listener<VALUE>; index: number };

  export type LifecycleName = NonNullable<{ [k in keyof Lifecycles<any>]: k }[keyof Lifecycles<any>]>;
  export type Lifecycle = NonNullable<{ [k in keyof Lifecycles<any>]: Lifecycles<any>[k] }[keyof Lifecycles<any>]>;
  export type Lifecycles<VALUE> = {
    listenerAdded?: Mitto<Mitto.Listener<VALUE>>;
    listenerRemoved?: Mitto<Mitto.Listener<VALUE>>;
    aborted?: Mitto<void>;
    listenersCount?: number;
  };
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
}

function test() {
  const MAX = 775_000_000;

  const start = performance.now();

  const mitto = new Mitto<number>();

  mitto.listen((value) => {
    if (value === MAX) console.log("L1", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
  });

  for (let i = 0; i <= MAX; i++) {
    mitto.emit(i);
  }
}

test(); // L1 775 000 000 1007 ms
