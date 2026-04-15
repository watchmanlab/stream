const NAME = "stream";

export class Stream<VALUE = void, NAME extends string = Stream.Name>
  implements AsyncIterable<VALUE, void, void>, Disposable
{
  protected listeners?: Stream.Listener<VALUE>[];
  protected hooks?: Stream.Hooks<VALUE>;
  readonly name: NAME;
  private source?: Stream.Source<VALUE>;
  constructor();
  constructor(name: NAME);
  constructor(source: Stream.Source<VALUE>);
  constructor(name: NAME, source: Stream.Source<VALUE>);
  constructor(nameOrSource?: NAME | Stream.Source<VALUE>, source?: Stream.Source<VALUE>) {
    if (typeof nameOrSource === "string") {
      this.name = nameOrSource;
      this.source = source;
    } else {
      this.name = NAME as NAME;
      this.source = nameOrSource;
    }
  }

  terminate() {
    if (!this.listeners) return;
    this.hooks?.beforeTerminate?.();
    this.push(Stream.TERMINATE as VALUE);
  }

  async *[Symbol.asyncIterator]() {
    let queue: VALUE[] | undefined = [];
    let resolve: Function | undefined = () => {};

    const abort = this.listen((value) => {
      queue?.push(value);
      resolve?.();
    });

    try {
      while (true) {
        if (queue.length) {
          const value = queue.shift()!;
          if (value === Stream.TERMINATE) break;
          yield value;
        } else {
          await new Promise<void>((r) => (resolve = r));
        }
      }
    } finally {
      abort();
      resolve();
      queue = undefined;
      resolve = undefined;
    }
  }
  [Symbol.dispose]() {
    this.terminate();
  }

  push(value: VALUE) {
    const newValues = this.hooks?.beforePush ? this.hooks.beforePush([value]) : [value];
    const listeners = this.listeners;

    if (!newValues || !listeners) {
      this.hooks?.afterValuesDropped?.(newValues ?? [value]);
      return;
    }

    if (newValues.length === 1) {
      const value = newValues[0];
      if (listeners) {
        const length = listeners.length;
        for (let i = 0; i < length; i++) {
          listeners[i](value);
        }
      }

      this.hooks?.afterPush?.(newValues);

      if (value === Stream.TERMINATE) {
        this.listeners = undefined;
        this.hooks?.afterTerminate?.();
      }
    } else {
      this.pushMany(newValues);
    }
  }
  pushMany(values: VALUE[]) {
    const newValues = this.hooks?.beforePush ? this.hooks.beforePush(values) : values;
    const listeners = this.listeners;

    if (!newValues || !listeners) {
      this.hooks?.afterValuesDropped?.(newValues ?? values);
      return;
    }

    let terminate = false;
    const valuesLenght = newValues.length;

    if (listeners) {
      const listenersLenght = listeners.length;
      for (let i = 0; i < listenersLenght; i++) {
        const fn = listeners[i];
        for (let j = 0; j < valuesLenght; j++) {
          const value = newValues[j];
          fn(value);
          if (value === Stream.TERMINATE) terminate = true;
        }
      }
    }

    this.hooks?.afterPush?.(newValues);

    if (terminate) {
      this.listeners = undefined;
      this.hooks?.afterTerminate?.();
    }
  }
  listen(fn: Stream.Listener<VALUE>, signal?: Stream.AnyStream): Stream.Abort {
    const self = this;
    signal?.listenOnce(abort);

    const listener = self.hooks?.beforeListenerAdded ? self.hooks.beforeListenerAdded(fn) : fn;
    if (!listener) return abort;

    if (!self.listeners) self.listeners = [];

    self.listeners.push(listener);

    self.hooks?.afterListenerAdded?.(listener);

    if (self.listeners.length > 1) return abort;

    self.hooks?.afterFirstListenerAdded?.(listener);

    let sourceGenerator:
      | AsyncGenerator<VALUE, void>
      | Generator<VALUE, void>
      | AsyncIterator<VALUE, void>
      | Iterator<VALUE, void>
      | undefined;

    let abortSource: Stream.Abort | undefined;

    if (self.source) {
      if (self.source instanceof Stream) {
        abortSource = self.source.listen((value) => self.push(value));
      } else if (typeof self.source === "function") {
        sourceGenerator = self.source();
      } else if (Symbol.asyncIterator in self.source) {
        sourceGenerator = self.source[Symbol.asyncIterator]();
      } else {
        sourceGenerator = self.source[Symbol.iterator]();
      }

      if (Symbol.asyncIterator in sourceGenerator!) {
        (async () => {
          for await (const value of sourceGenerator) {
            self.push(value);
          }
        })();
      } else if (Symbol.iterator in sourceGenerator!) {
        for (const value of sourceGenerator) {
          self.push(value);
        }
      }
    }

    return abort;
    function abort() {
      if (!listener || !self.listeners?.length) return;
      const index = self.listeners?.indexOf(listener) ?? -1;
      if (index === -1) return;

      self.listeners.splice(index, 1);

      self.hooks?.afterListenerRemoved?.(listener);

      if (self.listeners.length === 0) {
        sourceGenerator?.return?.();
        abortSource?.();
        self.listeners = undefined;
        self.hooks?.afterLastListenerRemoved?.(listener);
      }
    }
  }
  listenOnce(fn: Stream.Listener<VALUE>, signal?: Stream.AnyStream): void {
    const abort = this.listen((value) => {
      fn(value);
      abort();
    }, signal);
  }
  next(): Promise<VALUE> {
    return new Promise<VALUE>((resolve) => this.listenOnce(resolve));
  }
  pipe<OUTPUT_STREAM extends Stream.AnyStream>(transform: Stream.Transform<this, OUTPUT_STREAM>): OUTPUT_STREAM {
    return transform(this);
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type Listener<VALUE> = (value: VALUE) => any;
  export type Abort = () => void;
  export type Hooks<VALUE> = {
    beforeListenerAdded?: (fn: Stream.Listener<VALUE>) => Stream.Listener<VALUE> | void;
    afterListenerAdded?: (fn: Stream.Listener<VALUE>) => void;
    afterFirstListenerAdded?: (fn: Stream.Listener<VALUE>) => void;
    beforeListenerRemoved?: (fn: Stream.Listener<VALUE>) => void;
    afterListenerRemoved?: (fn: Stream.Listener<VALUE>) => void;
    afterLastListenerRemoved?: (fn: Stream.Listener<VALUE>) => void;
    beforePush?: (values: VALUE[]) => VALUE[] | void;
    afterPush?: (values: VALUE[]) => void;
    afterValuesDropped?: (values: VALUE[]) => void;
    beforeTerminate?: () => void;
    afterTerminate?: () => void;
  };
  export type AnyStream = Stream<any, any>;
  export type AnySource = Source<any>;
  export type AnySourceErr = SourceErr<any, any, AnyStream>;
  export type AnyTraversable = Traversable<AnyStream, AnyStream>;
  export type ExtractValue<T> =
    T extends Source<infer VALUE> ? VALUE : T extends SourceErr<infer VALUE, any, any> ? VALUE : T;
  export type ExtractName<T extends AnyStream | AnySourceErr> =
    T extends Stream<any, infer NAME> ? NAME : T extends AnySourceErr ? ExtractName<ExtractSourceErrStream<T>> : never;
  export type ExtractSentinel<T> = Extract<ExtractValue<T>, Sentinel>;
  export type ExtractCleanValue<T> = Exclude<ExtractValue<T>, Sentinel>;
  export type ExtractError<T extends Err<any> | AnySourceErr> =
    T extends Err<infer ERROR> ? ERROR : T extends SourceErr<any, infer ERROR, any> ? ERROR : never;
  export type ExtractSourceErr<T> = Extract<ExtractValue<T>, AnySourceErr>;
  export type ExcludeSourceErr<T> = Exclude<ExtractValue<T>, AnySourceErr>;
  export type ExtractSourceErrStream<T extends AnySourceErr> =
    T extends SourceErr<any, any, infer SOURCE> ? SOURCE : never;
  export type MaybeSourceErr<CLEAN_VALUE, ERROR, SOURCE extends AnyStream> = [ERROR] extends [never]
    ? never
    : SourceErr<CLEAN_VALUE, ERROR, SOURCE>;
  export type GeneratorFunction<VALUE> = () => AsyncGenerator<VALUE, void> | Generator<VALUE, void>;
  export type Source<VALUE> =
    | GeneratorFunction<VALUE>
    | AsyncIterable<VALUE, void>
    | Exclude<Iterable<VALUE, void>, string | String>;
  export type Transform<INPUT_STREAM extends AnyStream, OUTPUT_STREAM extends AnyStream> = (
    inputStream: INPUT_STREAM,
  ) => OUTPUT_STREAM;
  export type Traversable<OUTPUT_STREAM extends AnyStream, INPUT_STREAM extends AnyStream> = OUTPUT_STREAM &
    Record<ExtractName<INPUT_STREAM> | (`$${string}` & {}), INPUT_STREAM>;
  export function traversable<OUTPUT_STREAM extends AnyStream, INPUT_STREAM extends AnyStream>(
    outputStream: OUTPUT_STREAM,
    inputStream: INPUT_STREAM,
  ): Traversable<OUTPUT_STREAM, INPUT_STREAM> {
    return new Proxy(outputStream, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return inputStream;
      },
    }) as never;
  }
  export abstract class Sentinel {
    private readonly __sentinel = Symbol("*__sentinel#");
  }
  export function isSentinel<T extends Sentinel>(object: unknown): object is T {
    return object instanceof Sentinel;
  }
  export class SourceErr<VALUE, ERROR, SOURCE extends AnyStream> extends Stream.Sentinel {
    constructor(
      public readonly value: VALUE,
      public readonly error: ERROR,
      public readonly source: SOURCE,
    ) {
      super();
    }
    get sourceName(): SOURCE["name"] {
      return this.source.name;
    }
  }
  export class Err<ERROR> {
    constructor(public readonly value: ERROR) {}
  }
  export function err<ERROR>(value: ERROR): Err<ERROR> {
    return new Err(value);
  }
  export function isErr<ERROR>(object: unknown): object is Err<ERROR> {
    return object instanceof Err;
  }
  export function sourceErr<VALUE, ERROR, SOURCE extends AnyStream>({
    value,
    error,
    source,
  }: {
    value: VALUE;
    error: ERROR;
    source: SOURCE;
  }) {
    return new SourceErr(value, error, source);
  }
  export function isSourceErr<VALUE, ERROR, SOURCE extends AnyStream>(
    object: unknown,
  ): object is SourceErr<VALUE, ERROR, SOURCE> {
    return object instanceof SourceErr;
  }
  export const TERMINATE = Symbol("*TEMINATE#");
  export type Terminate = typeof TERMINATE;
  export function isTerminate(object: unknown): object is Terminate {
    return object === TERMINATE;
  }
  export const SKIP = Symbol("*SKIP#");
  export type Skip = typeof SKIP;
  export function isSkip(object: unknown): object is Skip {
    return object === SKIP;
  }
  export type Control = Terminate | Skip;
  export function isControl(object: unknown): object is Control {
    return object === SKIP || object === TERMINATE;
  }
}
