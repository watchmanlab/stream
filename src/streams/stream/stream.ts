export class Stream<VALUE, NAME extends string = Stream.Name> implements AsyncIterable<VALUE> {
  protected _consumers = new Map<VALUE[], { resolve: () => void; ready: Promise<void> }>();
  protected _source?: Stream.Source<VALUE>;
  protected _sourceGenerator?: AsyncGenerator<VALUE, void, Stream.Yielded>;
  protected _name = NAME as NAME;

  constructor();
  constructor(source: Stream.Source<VALUE>);
  constructor(source: Stream.Source<VALUE>, name: NAME);
  constructor(name: NAME);
  constructor(name: NAME, source: Stream.Source<VALUE>);

  constructor(sourceOrName1?: Stream.Source<VALUE> | NAME, sourceOrName2?: Stream.Source<VALUE>) {
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
  protected _requestNext(result: Stream.Yielded) {
    if (!this._requestingNext && this._sourceGenerator) {
      this._requestingNext = true;
      this._sourceGenerator.next(result).then((result) => {
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

    let result: Stream.Yielded;
    try {
      while (true) {
        if (queue.length) {
          const value = queue.shift()!;
          if (value === Stream.TERMINATE) break;
          result = yield value;
        } else {
          this._requestNext(result!);

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
        await this._sourceGenerator?.return?.();
        this._sourceGenerator = undefined;
      }
      this._onConsumerLeft?.();
      if (!result!?.ok) throw result!;

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

  static generator<VALUE>(source: Stream.Source<VALUE>): AsyncGenerator<VALUE, void, Stream.Yielded> {
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
  export type ValueOf<T extends Source<any>> = T extends Source<infer VALUE> ? VALUE : never;
  export type NameOf<T extends Stream<any, any>> = T extends Stream<any, infer NAME> ? NAME : never;
  export type GeneratorFunction<VALUE> = () => AsyncGenerator<VALUE, void, Yielded> | Generator<VALUE, void, Yielded>;
  export type Source<VALUE> =
    | GeneratorFunction<VALUE>
    | AsyncIterable<VALUE, void, Yielded>
    | Exclude<Iterable<VALUE, void, Yielded>, string | String>;
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

  export type Yielded = Yielded.Ok | Yielded.Err;

  export namespace Yielded {
    export class Ok {
      readonly ok = true;
      constructor(
        public readonly source: Stream<any, any>,
        public readonly value: unknown,
        public readonly data: unknown,
      ) {}
    }

    export class Err {
      readonly ok = false;
      constructor(
        public readonly source: Stream<any, any>,
        public readonly value: unknown,
        public readonly error: unknown,
      ) {}
    }

    export function ok({ source, value, data }: { source: Stream<any, any>; value: unknown; data: unknown }): Ok {
      return new Ok(source, value, data);
    }
    export function err({ source, value, error }: { source: Stream<any, any>; value: unknown; error: unknown }): Err {
      return new Err(source, value, error);
    }
    export function isOk(object: unknown): object is Ok {
      return object instanceof Ok;
    }
    export function isErr(object: unknown): object is Err {
      return object instanceof Err;
    }
  }
  export type Result<DATA, ERROR> = Result.Ok<DATA> | Result.Err<ERROR>;

  export namespace Result {
    export class Ok<DATA> {
      readonly ok = true;
      constructor(public readonly data: DATA) {}
    }
    export class Err<ERROR> {
      readonly ok = false;
      constructor(public readonly error: ERROR) {}
    }
    export function ok<DATA>(data: DATA): Ok<DATA> {
      return new Ok(data);
    }
    export function err<ERROR>(error: ERROR): Err<ERROR> {
      return new Err(error);
    }
    export function isOk<DATA>(object: unknown): object is Ok<DATA> {
      return object instanceof Ok;
    }
    export function isErr<ERROR>(object: unknown): object is Err<ERROR> {
      return object instanceof Err;
    }
  }
}

const USE_TRANSFORMER_INSIDE_PIPE_PLEASE = Symbol("*USE_TRANSFORMER_INSIDE_PIPE_PLEASE#");
