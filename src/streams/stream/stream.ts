export class Stream<VALUE, NAME extends string = Stream.Name, ERROR = unknown> implements AsyncIterable<VALUE> {
  protected _consumers = new Map<VALUE[], { resolve: () => void; ready: Promise<void> }>();
  protected _source?: Stream.Source<VALUE, ERROR, this>;
  protected _sourceGenerator?: AsyncGenerator<VALUE, void, Stream.MaybeError<VALUE, ERROR, this>>;
  protected _name = NAME as NAME;

  constructor();
  constructor(source: Stream.Source<VALUE, ERROR, Stream<VALUE, any, ERROR>>);
  constructor(source: Stream.Source<VALUE, ERROR, Stream<VALUE, any, ERROR>>, name: NAME);
  constructor(name: NAME);
  constructor(name: NAME, source: Stream.Source<VALUE, ERROR, Stream<VALUE, any, ERROR>>);

  constructor(
    sourceOrName1?: Stream.Source<VALUE, ERROR, Stream<VALUE, any, ERROR>> | NAME,
    sourceOrName2?: Stream.Source<VALUE, ERROR, Stream<VALUE, any, ERROR>>,
  ) {
    if (typeof sourceOrName1 === "string" || sourceOrName1 instanceof String) {
      this._name = (sourceOrName1 as NAME) ?? NAME;
      this._source = sourceOrName2;
    } else {
      this._name = (sourceOrName2 as NAME) ?? NAME;
      this._source = sourceOrName1;
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
      then: (resolve?: () => void, reject?: () => void) =>
        new Promise<void>((r) => setTimeout(r, 0)).then(resolve, reject),
    };
  }
  protected _requestingNext = false;
  protected _requestNext(maybeError: Stream.MaybeError<VALUE, ERROR, this>) {
    if (!this._requestingNext && this._sourceGenerator) {
      this._requestingNext = true;
      this._sourceGenerator.next(maybeError).then((result) => {
        this._requestingNext = false;
        if (result.done) return;
        this.push(result.value);
      });
    }
  }

  protected _onConsumerJoin?: () => void;
  protected _onConsumerLeft?: () => void;
  async *[Symbol.asyncIterator]() {
    if (this._consumers.size === 0 && this._source) {
      this._sourceGenerator = Stream.generator(this._source);
    }

    const queue: VALUE[] = [];
    this._consumers.set(queue, { resolve() {}, ready: Promise.resolve() });
    this._onConsumerJoin?.();

    let ready: () => void;

    let maybeError: Stream.MaybeError<VALUE, ERROR, this>;
    try {
      while (true) {
        if (queue.length) {
          const value = queue.shift()!;
          if (value === Stream.TERMINATE) break;
          maybeError = yield value as VALUE;
        } else {
          this._requestNext(maybeError);
          await new Promise<void>((resolve) => {
            this._consumers.set(queue, {
              resolve,
              ready: new Promise<void>((r) => (ready = r)),
            });
          });
        }

        ready!?.();
      }
    } finally {
      this._consumers.get(queue)?.resolve();
      this._consumers.delete(queue);

      queue.length = 0;

      if (this._consumers.size === 0) {
        this._sourceGenerator?.return?.();
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
    transformer: Stream.Transformer<CUSTOM_NAME, this, OUTPUT>,
  ): Stream.PipeResult<OUTPUT, NAME, this>;
  pipe<CUSTOM_NAME extends string, OUTPUT extends Stream<any, CUSTOM_NAME>>(
    transformer: Stream.Transformer<CUSTOM_NAME, this, OUTPUT>,
    name: CUSTOM_NAME,
  ): Stream.PipeResult<OUTPUT, NAME, this>;
  pipe<CUSTOM_NAME extends string, OUTPUT extends Stream<any, CUSTOM_NAME>>(
    name: CUSTOM_NAME,
    transformer: Stream.Transformer<CUSTOM_NAME, this, OUTPUT>,
  ): Stream.PipeResult<OUTPUT, NAME, this>;
  pipe(transformerOrName1: Function | string, transformerOrName2?: Function | string) {
    const output =
      typeof transformerOrName1 === "string" && typeof transformerOrName2 === "function"
        ? (transformerOrName2!(USE_TRANSFORMER_INSIDE_PIPE_PLEASE, this, transformerOrName1) as any)
        : typeof transformerOrName1 === "function"
          ? (transformerOrName1!(USE_TRANSFORMER_INSIDE_PIPE_PLEASE, this, transformerOrName2) as any)
          : void 0;

    if (this._name in output) {
      throw new Error(
        `Naming conflict: Cannot name ${this.constructor.name} transformer with "${this._name}" ` +
          `because ${output.constructor.name} already has a property with that name.\n` +
          `Solutions:\n` +
          `  1. Use different name in pipe: .pipe("$${this._name}", ${this.constructor.name})\n` +
          `  2. Rename on stream creation: new Stream<T, "$${this._name}">()\n` +
          `conflictingProperty:${output[this._name]};`,
      );
    }
    output[this._name] = this;

    const self = this;
    return new Proxy(output, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);

        return target[NAME] || self;
      },
    });
  }

  static generator<VALUE, ERROR, SOURCE extends Stream<VALUE, any, ERROR> | undefined>(
    source: Stream.Source<VALUE, ERROR, SOURCE>,
  ): AsyncGenerator<VALUE, void, Stream.MaybeError<VALUE, ERROR, SOURCE>> {
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
const NAME = "root";

export namespace Stream {
  export type Name = typeof NAME;
  export type ValueOf<T extends Source<any, any, any>> = T extends Source<infer VALUE, any, any> ? VALUE : never;
  export type ErrorOf<T extends Source<any, any, any>> = T extends Source<any, infer ERROR, any> ? ERROR : never;
  export type NameOf<T extends Stream<any, any, any>> = T extends Stream<any, infer NAME, any> ? NAME : never;
  export type GeneratorFunction<VALUE, ERROR, SOURCE extends Stream<VALUE, any, ERROR> | undefined> = () =>
    | AsyncGenerator<VALUE, void, MaybeError<VALUE, ERROR, SOURCE>>
    | Generator<VALUE, void, MaybeError<VALUE, ERROR, SOURCE>>;
  export type Source<VALUE, ERROR, SOURCE extends Stream<VALUE, any, ERROR> | undefined> =
    | GeneratorFunction<VALUE, ERROR, SOURCE>
    | AsyncIterable<VALUE, any, MaybeError<VALUE, ERROR, SOURCE>>
    | Exclude<Iterable<VALUE, any, MaybeError<VALUE, ERROR, SOURCE>>, string | String>;
  export type PushResult = {
    readonly awaitBroadcast: Promise<void>;
    readonly awaitAllConsumers: Promise<void[]>;
    readonly awaitAnyConsumer: Promise<void>;
    then: (resolve?: (() => void) | undefined, reject?: (() => void) | undefined) => Promise<void>;
  };
  export type Transformer<NAME extends string, INPUT extends Stream<any, any>, OUTPUT extends Stream<any, NAME>> = (
    useTransformerInsidePipePlease: UseTransformerInsidePipePlease,
    stream: INPUT,
    name?: NAME,
  ) => OUTPUT;
  export type Traversable<NAME extends string, INPUT extends Stream<any, any>> = Record<
    NAME | (`$${string}` & {}),
    INPUT
  >;
  export type PipeResult<
    OUTPUT extends Stream<any, any>,
    NAME extends string,
    INPUT extends Stream<any, any>,
  > = NAME extends keyof OUTPUT
    ? {
        error: `Naming conflict: "${NAME}" already exists in ${OUTPUT["name"]}`;
        suggestion: `Use .pipe("$${NAME}", transformer) or rename the stream`;
        conflictingProperty: OUTPUT[NAME];
      }
    : OUTPUT & Traversable<NAME, INPUT>;

  export const TERMINATE = Symbol("**TERMINATE##");
  export type Terminate = typeof TERMINATE;
  export type UseTransformerInsidePipePlease = typeof USE_TRANSFORMER_INSIDE_PIPE_PLEASE;

  export class Error<VALUE, ERROR, SOURCE extends Stream<VALUE, any, ERROR> | undefined> {
    public readonly name = "StreamError";
    public readonly cause: ERROR;
    public readonly stack?: string;
    public readonly message: string;

    constructor(
      public readonly error: ERROR,
      public readonly value: VALUE,
      public readonly source: SOURCE,
    ) {
      this.cause = error;
      this.message = error instanceof globalThis.Error ? error.message : String(error);

      if (error instanceof globalThis.Error && error.stack) {
        this.stack = error.stack;
      } else if (globalThis.Error.captureStackTrace) {
        globalThis.Error.captureStackTrace(this, Error);
      }
    }

    static isError<VALUE, ERROR, SOURCE extends Stream<VALUE, any, ERROR> | undefined>(
      obj: unknown,
    ): obj is Error<VALUE, ERROR, SOURCE> {
      return obj instanceof Error;
    }

    toString() {
      return `${this.name}: ${this.message}`;
    }
  }
  export type MaybeError<VALUE, ERROR, SOURCE extends Stream<VALUE, any, ERROR> | undefined> =
    | Error<VALUE, ERROR, SOURCE>
    | undefined
    | void
    | null;
}

const USE_TRANSFORMER_INSIDE_PIPE_PLEASE = Symbol("*USE_TRANSFORMER_INSIDE_PIPE_PLEASE#");
