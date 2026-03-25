const NAME = "root";
export class Stream<VALUE, NAME extends string = Stream.Name> implements AsyncIterable<VALUE> {
  protected _consumers = new Map<VALUE[], { resolve: () => void; ready: Promise<void> }>();
  protected _source?: Stream.Source<VALUE>;
  protected _sourceGenerator?: AsyncGenerator<VALUE, void, unknown>;
  protected _name = NAME as NAME;
  constructor();
  constructor(source: Stream.Source<VALUE>);
  constructor(name: NAME);
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
  pipe<CUSTOM_NAME extends string, OUTPUT extends Stream<any, CUSTOM_NAME>>(
    transformer: Stream.Transform<this, CUSTOM_NAME, OUTPUT>,
  ): Stream.Transformer<OUTPUT, NAME, this>;
  pipe<CUSTOM_NAME extends string, OUTPUT extends Stream<any, CUSTOM_NAME>>(
    name: CUSTOM_NAME,
    transformer: Stream.Transform<this, CUSTOM_NAME, OUTPUT>,
  ): Stream.Transformer<OUTPUT, NAME, this>;
  pipe(transformerOrName1: Function | string, transformer?: Function) {
    const output =
      typeof transformerOrName1 === "string"
        ? (transformer!({
            token: USE_TRANSFORMER_INSIDE_PIPE_PLEASE,
            inputStream: this,
            name: transformerOrName1,
          }) as any)
        : typeof transformerOrName1 === "function"
          ? (transformerOrName1!({ token: USE_TRANSFORMER_INSIDE_PIPE_PLEASE, inputStream: this }) as any)
          : void 0;

    return new Proxy(output, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);

        return this;
      },
    });
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
  export type ExtractValue<T> =
    T extends Source<infer VALUE> ? VALUE : T extends SourceErr<infer VALUE, any, any> ? VALUE : T;
  export type ExtractName<T extends Stream<any, any> | SourceErr<any, any, any>> =
    T extends Stream<any, infer NAME> ? NAME : T extends SourceErr<any, any, any> ? ExtractName<T["source"]> : never;
  export type ExtractSentinel<T> = Extract<ExtractValue<T>, Sentinel>;
  export type ExtractCleanValue<T> = Exclude<ExtractValue<T>, Sentinel>;
  export type ExtractError<T extends Err<any> | SourceErr<any, any, any>> =
    T extends Err<infer ERROR> ? ERROR : T extends SourceErr<any, infer ERROR, any> ? ERROR : never;
  export type ExtractSourceErr<T> = Extract<ExtractValue<T>, SourceErr<any, any, any>>;
  export type ExcludeSourceErr<T> = Exclude<ExtractValue<T>, SourceErr<any, any, any>>;
  export type ExtractSource<SOURCE_ERR extends SourceErr<any, any, any>> =
    SOURCE_ERR extends SourceErr<any, any, infer SOURCE> ? SOURCE : never;
  export type MaybeSourceErr<CLEAN_VALUE, ERROR, SOURCE extends Stream<any, any>> = [ERROR] extends [never]
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
  export type TransformOptions<INPUT_STREAM extends Stream<any, any>, OUTPUT_NAME extends string> = {
    token: UseTransformerInsidePipePlease;
    inputStream: INPUT_STREAM;
    name?: OUTPUT_NAME;
  };
  export type Transform<
    INPUT_STREAM extends Stream<any, any>,
    OUTPUT_NAME extends string,
    OUTPUT_STREAM extends Stream<any, OUTPUT_NAME>,
  > = (options: TransformOptions<INPUT_STREAM, OUTPUT_NAME>) => OUTPUT_STREAM;

  export type Transformer<
    OUTPUT_STREAM extends Stream<any, any>,
    INPUT_NAME extends string,
    INPUT_STREAM extends Stream<any, INPUT_NAME>,
  > = OUTPUT_STREAM & Record<INPUT_NAME | (`$${string}` & {}), INPUT_STREAM>;
  export abstract class Sentinel {
    private readonly __sentinel = Symbol("__sentinel");
  }
  export function isSentinel<T extends Sentinel>(object: unknown): object is T {
    return object instanceof Sentinel;
  }
  export const TERMINATE = Symbol("*TEMINATE#");
  export type Terminate = typeof TERMINATE;
  export const SKIP = Symbol("*SKIP#");
  export type Skip = typeof SKIP;
  export type ErrorEvent<CLEAN_VALUE, ERROR, SOURCE extends Stream<any, any>> =
    | {
        type: "expected";
        value: CLEAN_VALUE;
        error: ERROR;
        source: SOURCE;
      }
    | {
        type: "unexpected";
        value: CLEAN_VALUE;
        error: unknown;
        source: SOURCE;
      };
  export class SourceErr<CLEAN_VALUE, ERROR, SOURCE extends Stream<any, any>> extends Stream.Sentinel {
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
  export function sourceErr<CLEAN_VALUE, ERROR, SOURCE extends Stream<any, any>>({
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
  export function isSourceErr<CLEAN_VALUE, ERROR, SOURCE extends Stream<any, any>>(
    object: unknown,
  ): object is SourceErr<CLEAN_VALUE, ERROR, SOURCE> {
    return object instanceof SourceErr;
  }
  export const EMPTY = Symbol("*EMPTY#");
  export type Empty = typeof EMPTY;
  export type UseTransformerInsidePipePlease = typeof USE_TRANSFORMER_INSIDE_PIPE_PLEASE;
}

const USE_TRANSFORMER_INSIDE_PIPE_PLEASE = Symbol("*USE_TRANSFORMER_INSIDE_PIPE_PLEASE#");
