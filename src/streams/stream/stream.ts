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

    try {
      while (true) {
        if (queue.length) {
          const value = queue.shift()!;
          if (value === Stream.TERMINATE) break;
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
    transformer: Stream.Transformer<CUSTOM_NAME, this, OUTPUT>,
  ): Stream.PipeResult<NAME, this, OUTPUT>;
  pipe<CUSTOM_NAME extends string, OUTPUT extends Stream<any, CUSTOM_NAME>>(
    transformer: Stream.Transformer<CUSTOM_NAME, this, OUTPUT>,
    name: CUSTOM_NAME,
  ): Stream.PipeResult<NAME, this, OUTPUT>;
  pipe<CUSTOM_NAME extends string, OUTPUT extends Stream<any, CUSTOM_NAME>>(
    name: CUSTOM_NAME,
    transformer: Stream.Transformer<CUSTOM_NAME, this, OUTPUT>,
  ): Stream.PipeResult<NAME, this, OUTPUT>;
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
  export type ValueOf<T extends Source<any>> = T extends Source<infer VALUE> ? VALUE : never;
  export type NameOf<T extends Stream<any, any>> = T extends Stream<any, infer NAME> ? NAME : never;

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
    NAME extends string,
    INPUT extends Stream<any, NAME>,
    OUTPUT extends Stream<any, any>,
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
  export type ErrorEvent<VALUE, ERROR, SOURCE extends Stream<any, any>> =
    | { type: "expected"; source: SOURCE; value: VALUE; detail: ERROR }
    | { type: "unexpected"; source: SOURCE; value: VALUE; detail: unknown };
  export type Result<VALUE, ERROR, SOURCE extends Stream<any, any>> =
    | Result.Ok<VALUE>
    | Result.Err<ERROR>
    | Result.SourceErr<VALUE, ERROR, SOURCE>;

  export namespace Result {
    export class SourceErr<VALUE, ERROR, SOURCE extends Stream<any, any>> {
      constructor(
        public readonly source: SOURCE,
        public readonly value: VALUE,
        public readonly detail: ERROR,
      ) {}
    }
    export class Ok<VALUE> {
      constructor(public readonly value: VALUE) {}
    }
    export class Err<VALUE> {
      constructor(public readonly value: VALUE) {}
    }
    export function ok<VALUE>(value: VALUE): Ok<VALUE> {
      return new Ok(value);
    }
    export function err<VALUE>(value: VALUE): Err<VALUE> {
      return new Err(value);
    }
    export function sourceErr<VALUE, ERROR, SOURCE extends Stream<any, any>>({
      source,
      value,
      detail,
    }: {
      source: SOURCE;
      value: VALUE;
      detail: ERROR;
    }) {
      return new SourceErr(source, value, detail);
    }
    export function isOk<VALUE>(object: unknown): object is Ok<VALUE> {
      return object instanceof Ok;
    }
    export function isErr<VALUE>(object: unknown): object is Err<VALUE> {
      return object instanceof Err;
    }
    export function isSourceErr<VALUE, ERROR, SOURCE extends Stream<any, any>>(
      object: unknown,
    ): object is SourceErr<VALUE, ERROR, SOURCE> {
      return object instanceof SourceErr;
    }
  }
}

const USE_TRANSFORMER_INSIDE_PIPE_PLEASE = Symbol("*USE_TRANSFORMER_INSIDE_PIPE_PLEASE#");
