export class Stream<VALUE, NAME extends string = Stream.Name> implements AsyncIterable<VALUE> {
  protected _consumers = new Map<VALUE[], () => void>();
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

  async push(value: VALUE, ...values: VALUE[]) {
    for (const [queue, resolver] of this._consumers) {
      queue.push(value, ...values);
      resolver();
    }

    //TODO: return a stream that track the broadcast or finished jobs
    await new Promise((r) => setTimeout(r));
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

    const queue: VALUE[] = [];

    try {
      while (true) {
        if (queue.length) {
          yield queue.shift()!;
        } else {
          this._requestNext();
          await new Promise<void>((resolve) => {
            this._consumers.set(queue, resolve);
          });
        }
      }
    } finally {
      this._consumers.delete(queue);
      queue.length = 0;
      if (this._consumers.size === 0) {
        this._sourceGenerator?.return?.();
        this._sourceGenerator = undefined;
      }
      return;
    }
  }
  async *generator(signal?: Stream<any, any>) {
    const iter = this[Symbol.asyncIterator]();
    let aborted = false;
    signal?.next().then(() => {
      aborted = true;
      iter.return();
    });
    for await (const value of iter) {
      if (aborted) break;
      yield value;
    }
  }
  async next(): Promise<VALUE> {
    for await (const value of this) {
      return value;
    }
    return undefined as never;
  }

  listen(): Stream<void, "signal">;
  listen(callback: (value: VALUE, signal: Stream<void, "signal">) => void): Stream<void, "signal">;
  listen(
    callback: (value: VALUE, signal: Stream<void, "signal">) => void,
    signal: Stream<any, any>,
  ): Stream<void, "signal">;
  listen(signal: Stream<any, any>): Stream<void, "signal">;
  listen(
    signal: Stream<any, any>,
    callback: (value: VALUE, signal: Stream<void, "signal">) => void,
  ): Stream<void, "signal">;
  listen(
    callbackOrSignal1?: ((value: VALUE, signal: Stream<void, "signal">) => void) | Stream<any, any>,
    callbackOrSignal2?: ((value: VALUE, signal: Stream<void, "signal">) => void) | Stream<any, any>,
  ): Stream<void, "signal"> {
    const [callback, remotSignal] =
      typeof callbackOrSignal1 === "function"
        ? [callbackOrSignal1 as Function | undefined, callbackOrSignal2 as Stream<any, any> | undefined]
        : [callbackOrSignal2 as Function | undefined, callbackOrSignal1 as Stream<any, any> | undefined];

    const generator = this[Symbol.asyncIterator]();
    let aborted = false;

    const signal = new Stream<void, "signal">();

    signal.next().then(() => {
      aborted = true;
      generator.return();
    });

    remotSignal?.next().then(() => {
      aborted = true;
      signal.push();
    });

    (async () => {
      for await (const value of generator) {
        if (aborted) break;

        callback?.(value, signal);
      }
    })();
    return signal;
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
