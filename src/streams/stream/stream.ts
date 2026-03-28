const NAME = "root";
export class Stream<VALUE, NAME extends string = Stream.Name> implements AsyncIterable<VALUE> {
  protected _consumers = new Map<VALUE[], { resolve: () => void; ready: Promise<void> }>();
  protected _source?: Stream.Source<VALUE>;
  protected _sourceGenerator?: AsyncGenerator<VALUE, void, unknown>;
  protected _name = NAME as NAME;
  constructor();
  constructor(name: NAME);
  constructor(source: Stream.Source<VALUE>);
  constructor(name: NAME, source: Stream.Source<VALUE>);
  constructor(sourceOrName?: Stream.Source<VALUE> | NAME, source?: Stream.Source<VALUE>) {
    if (typeof sourceOrName === "string" || sourceOrName instanceof String) {
      this._name = (sourceOrName as NAME) ?? NAME;
      this._source = source;
    } else {
      this._source = sourceOrName;
    }
  }
  get name() {
    return this._name;
  }
  push(value: VALUE, ...values: VALUE[]): Stream.PushResult {
    const readyPromises = new Array<Promise<void>>();

    for (const [queue, { resolve, ready }] of this._consumers) {
      queue.push(value, ...values);
      resolve();
      readyPromises.push(ready);
    }

    return {
      get awaitBroadcast() {
        return new Promise<void>((r) => setTimeout(r, 0));
      },
      get awaitAllConsumers() {
        return Promise.all(readyPromises);
      },
      get awaitAnyConsumer() {
        return Promise.any(readyPromises);
      },
      then: (resolve?: () => void, reject?: () => void) => Promise.resolve().then(resolve, reject),
    };
  }
  protected _requestingNext = false;
  protected _requestNext() {
    if (!this._requestingNext && this._sourceGenerator) {
      this._requestingNext = true;
      this._sourceGenerator.next().then((result) => {
        this._requestingNext = false;
        if (result.done) {
          this.push(Stream.TERMINATE as VALUE);
          return;
        }
        this.push(result.value);
      });
    }
  }
  protected _onConsumerJoin?: (queue: VALUE[]) => void;
  protected _onConsumerLeft?: () => void;
  async *[Symbol.asyncIterator]() {
    if (this._consumers.size === 0 && this._source) {
      this._sourceGenerator = Stream.generator(this._source);
    }

    const queue: VALUE[] = [];
    this._consumers.set(queue, { resolve() {}, ready: Promise.resolve() });
    this._onConsumerJoin?.(queue);

    let ready: () => void;

    try {
      while (true) {
        if (queue.length) {
          const value = queue.shift()!;
          if (value === Stream.TERMINATE) {
            break;
          } else if (value === Stream.SKIP) {
            continue;
          }

          yield value;
        } else {
          ready!?.();
          this._requestNext();

          await new Promise<void>((resolve) => {
            this._consumers.set(queue, {
              resolve,
              ready: new Promise<void>((r) => (ready = r)),
            });
          });
        }
      }
    } finally {
      this._consumers.get(queue)?.resolve();
      this._consumers.delete(queue);

      queue.length = 0;

      if (this._consumers.size === 0) {
        await this._sourceGenerator?.return?.();
        this._sourceGenerator = undefined;
      }
      this._onConsumerLeft?.();

      return;
    }
  }
  next(): Promise<IteratorResult<Awaited<VALUE>, void>> {
    return this[Symbol.asyncIterator]().next();
  }
  pipe<OUTPUT_STREAM extends Stream.AnyStream>(transform: Stream.Transform<this, OUTPUT_STREAM>): OUTPUT_STREAM {
    return transform(this);
  }

  static generator<VALUE>(source: Stream.Source<VALUE>): AsyncGenerator<VALUE, void, unknown> {
    return (async function* () {
      if (!source) return;
      if (Symbol.asyncIterator in source || Symbol.iterator in source) {
        yield* source;
      } else if (typeof source === "function") {
        yield* source();
      }
    })();
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type AnyStream<T = any, N extends string = string> = Stream<T, N>;
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
  export type GeneratorFunction<VALUE> = () => AsyncGenerator<VALUE, void, unknown> | Generator<VALUE, void, unknown>;
  export type Source<VALUE> =
    | GeneratorFunction<VALUE>
    | AsyncIterable<VALUE, void, unknown>
    | Exclude<Iterable<VALUE, void, unknown>, string | String>;
  export type PushResult = {
    readonly awaitBroadcast: Promise<void>;
    readonly awaitAllConsumers: Promise<void[]>;
    readonly awaitAnyConsumer: Promise<void>;
    then: (resolve?: (() => void) | undefined, reject?: (() => void) | undefined) => Promise<void>;
  };

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
  export type ErrorEvent<CLEAN_VALUE, ERROR> =
    | {
        type: "expected";
        value: CLEAN_VALUE;
        error: ERROR;
      }
    | {
        type: "unexpected";
        value: CLEAN_VALUE;
        error: unknown;
      };
  export class SourceErr<CLEAN_VALUE, ERROR, SOURCE extends AnyStream> extends Stream.Sentinel {
    constructor(
      public readonly value: CLEAN_VALUE,
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
  export function sourceErr<CLEAN_VALUE, ERROR, SOURCE extends AnyStream>({
    value,
    error,
    source,
  }: {
    value: CLEAN_VALUE;
    error: ERROR;
    source: SOURCE;
  }) {
    return new SourceErr(value, error, source);
  }
  export function isErr<ERROR>(object: unknown): object is Err<ERROR> {
    return object instanceof Err;
  }
  export function isSourceErr<CLEAN_VALUE, ERROR, SOURCE extends AnyStream>(
    object: unknown,
  ): object is SourceErr<CLEAN_VALUE, ERROR, SOURCE> {
    return object instanceof SourceErr;
  }
  export const EMPTY = Symbol("*EMPTY#");
  export type Empty = typeof EMPTY;
  export const TERMINATE = Symbol("*TEMINATE#");
  export type Terminate = typeof TERMINATE;
  export const SKIP = Symbol("*SKIP#");
  export type Skip = typeof SKIP;
}
