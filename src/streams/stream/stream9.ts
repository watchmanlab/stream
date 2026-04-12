const NAME = "stream";

export class Stream<VALUE, NAME extends string = Stream.Name> {
  protected listeners: ((value: VALUE) => void)[] = [];

  constructor(public readonly name: NAME = NAME as NAME) {}

  protected listenerAdded?: () => void;
  protected listenerRemoved?: () => void;
  protected listenerEmptied?: () => void;

  push(...values: VALUE[]) {
    const listenersLenght = this.listeners.length;
    const valuesLenght = values.length;
    let terminate = false;
    for (let i = 0; i < listenersLenght; i++) {
      const fn = this.listeners[i];
      for (let j = 0; j < valuesLenght; j++) {
        const value = values[j];
        if (value === Stream.TERMINATE) terminate = true;
        fn(values[j]);
      }
    }
    if (terminate) this.listeners.length = 0;
  }

  listen(fn: (value: VALUE) => void) {
    this.listenerAdded?.();

    const self = this;

    this.listeners.push(fn);

    return abort;

    function abort() {
      self.listeners = self.listeners.filter((listener) => listener !== fn);
      self.listenerRemoved?.();
      if (self.listeners.length === 0) self.listenerEmptied?.();
    }
  }
  next(fn: (value: VALUE) => void) {
    const abort = this.listen((value) => {
      fn(value);
      abort();
    });
  }
  pipe<OUTPUT_STREAM extends Stream.AnyStream>(transform: Stream.Transform<this, OUTPUT_STREAM>): OUTPUT_STREAM {
    return transform(this);
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type AnyStream = Stream<any, any>;
  export type AnySourceErr = SourceErr<any, any, AnyStream>;
  export type AnyTraversable = Traversable<AnyStream, AnyStream>;
  export type ExtractValue<T> =
    T extends Stream<infer VALUE> ? VALUE : T extends SourceErr<infer VALUE, any, any> ? VALUE : T;
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
  export function decorate<STREAM extends Stream.AnyStream, PROPS extends Record<string, unknown>>(
    stream: STREAM,
    props: PROPS,
  ): STREAM & PROPS {
    return Object.defineProperties(stream, Object.getOwnPropertyDescriptors(props)) as STREAM & PROPS;
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
  export function isErr<ERROR>(object: unknown): object is Err<ERROR> {
    return object instanceof Err;
  }
  export function isSourceErr<VALUE, ERROR, SOURCE extends AnyStream>(
    object: unknown,
  ): object is SourceErr<VALUE, ERROR, SOURCE> {
    return object instanceof SourceErr;
  }
  // export const EMPTY = Symbol("*EMPTY#");
  // export type Empty = typeof EMPTY;
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
