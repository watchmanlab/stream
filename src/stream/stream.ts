export class Stream<VALUE, NAME extends string = Stream.Name> implements AsyncIterable<VALUE> {
  protected _consumers = new Set<{ send: (values: VALUE[]) => void; ready: Promise<void> }>();
  protected _source?: Stream.Source<VALUE>;
  protected _sourceGenerator?: AsyncGenerator<VALUE, void>;
  protected _name = NAME as NAME;
  constructor();
  constructor(source: Stream.Source<VALUE>);
  constructor(source: Stream.Source<VALUE>, name: NAME);
  constructor(name: NAME);
  constructor(name: NAME, source: Stream.Source<VALUE>);

  constructor(sourceOrName1?: Stream.Source<VALUE> | NAME, sourceOrName2?: Stream.Source<VALUE>) {
    if (typeof sourceOrName1 === "string" || sourceOrName1 instanceof String) {
      this._name ??= sourceOrName1 as NAME;
      this._source = sourceOrName2;
    } else {
      this._name ??= sourceOrName2 as NAME;
      this._source = sourceOrName1;
    }
  }

  get name() {
    return this._name;
  }

  push(value: VALUE, ...values: VALUE[]) {
    const readyPromises = new Array<Promise<void>>();
    for (const consumer of this._consumers) {
      consumer.send([value, ...values]);
      readyPromises.push(consumer.ready);
    }

    return {
      get awaitBroadcast() {
        return new Promise((r) => setTimeout(r, 0));
      },
      get awaitAll() {
        return Promise.all(readyPromises);
      },
      get awaitAny() {
        return Promise.any(readyPromises);
      },
      then: (resolve?: () => void, reject?: () => void) => new Promise((r) => setTimeout(r, 0)).then(resolve, reject),
    };
  }

  protected _requestingNext = false;

  protected _requestNext() {
    if (!this._requestingNext && this._sourceGenerator) {
      this._requestingNext = true;
      this._sourceGenerator.next().then((result) => {
        this._requestingNext = false;
        if (result.done) return;
        this.push(result.value);
      });
    }
  }

  async *[Symbol.asyncIterator]() {
    if (this._consumers.size === 0 && this._source) {
      this._sourceGenerator = Stream.generator(this._source);
    }

    let ready: () => void;
    let consumer;
    try {
      while (true) {
        this._requestNext();
        yield* await new Promise<VALUE[]>((r) => {
          consumer = { send: r, ready: new Promise<void>((r) => (ready = r)) };
          this._consumers.add(consumer);
        });
        ready!?.();
        this._consumers.delete(consumer!);
      }
    } finally {
      this._consumers.delete(consumer!);
      if (this._consumers.size === 0) {
        this._sourceGenerator?.return?.();
        this._sourceGenerator = undefined;
      }
      return;
    }
  }
  async *generator(...signals: Stream<any, any>[]) {
    const iter = this[Symbol.asyncIterator]();
    let aborted = false;
    signals.map((signal) =>
      signal.next().then(() => {
        aborted = true;
        iter.return();
      }),
    );
    for await (const value of iter) {
      if (aborted) break;
      yield value;
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
  static generator<VALUE>(source: Stream.Source<VALUE>): AsyncGenerator<VALUE, void, any> {
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
const NAME = "source";

export namespace Stream {
  export type Name = typeof NAME;
  export type ValueOf<T extends Source<any>> = T extends Stream<infer VALUE> ? VALUE : never;
  export type NameOf<T extends Stream<any, any>> = T extends Stream<any, infer NAME> ? NAME : never;
  export type GeneratorFunction<VALUE> = () => AsyncGenerator<VALUE> | Generator<VALUE>;
  export type Source<VALUE> =
    | GeneratorFunction<VALUE>
    | AsyncIterable<VALUE>
    | Exclude<Iterable<VALUE>, string | String>;
  export type Transformer<NAME extends string, INPUT extends Stream<any, any>, OUTPUT extends Stream<any, NAME>> = (
    useTransformerInsidePipePlease: UseTransformerInsidePipePlease,
    stream: INPUT,
    name?: NAME,
  ) => OUTPUT;
  export type Traversable<NAME extends string, STREAM extends Stream<any, any>> = Record<NAME | (string & {}), STREAM>;
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
  export type UseTransformerInsidePipePlease = typeof USE_TRANSFORMER_INSIDE_PIPE_PLEASE;
}

const USE_TRANSFORMER_INSIDE_PIPE_PLEASE = Symbol("*USE_TRANSFORMER_INSIDE_PIPE_PLEASE#");
