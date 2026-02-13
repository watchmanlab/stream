export class Stream<VALUE, NAME extends string = "root"> implements AsyncIterable<VALUE> {
  protected _consumers = new Map<VALUE[], () => void>();
  protected _source?: Stream.Source<VALUE>;
  protected _name = "root" as NAME;
  constructor();
  constructor(source: Stream.Source<VALUE>);
  constructor(source: Stream.Source<VALUE>, name: NAME);
  constructor(name: NAME);
  constructor(name: NAME, source: Stream.Source<VALUE>);

  constructor(sourceOrName1?: Stream.Source<VALUE> | NAME, sourceOrName2?: Stream.Source<VALUE>) {
    if (typeof sourceOrName1 === "string" || sourceOrName1 instanceof String) {
      this._name = (sourceOrName1 ?? "root") as NAME;
      this._source = sourceOrName2;
    } else {
      this._name = (sourceOrName2 ?? "root") as NAME;
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

  protected requestingNext = false;

  async *[Symbol.asyncIterator]() {
    const generator = this._source ? Stream.generator(this._source) : undefined;

    const queue: VALUE[] = [];

    try {
      while (true) {
        if (queue.length) {
          yield queue.shift()!;
        } else {
          if (!this.requestingNext) {
            this.requestingNext = true;
            generator?.next().then((result) => {
              this.requestingNext = false;
              if (result.done) return;
              this.push(result.value);
            });
          }
          await new Promise<void>((resolve) => {
            this._consumers.set(queue, resolve);
          });
        }
      }
    } finally {
      this._consumers.delete(queue);
      queue.length = 0;
      generator?.return?.();
      return;
    }
  }
  async *generator(controller?: Controller) {
    const iter = this[Symbol.asyncIterator]();
    controller?.next().then(() => iter.return());
    for await (const value of iter) {
      if (controller?.aborted) break;
      yield value;
    }
  }
  async next(): Promise<VALUE> {
    for await (const value of this) {
      return value;
    }
    return undefined as never;
  }

  listen(callback?: (value: VALUE, controller: Controller) => void): Controller {
    const generator = this[Symbol.asyncIterator]();

    const controller = new Controller(async () => {
      await generator.return();
    });

    (async () => {
      for await (const value of generator) {
        if (controller.aborted) break;

        callback?.(value, controller);
      }
    })();
    return controller;
  }
  pipe<CUSTOM_NAME extends string, OUTPUT extends Stream<any, CUSTOM_NAME>>(
    transformer: Stream.Transformer<CUSTOM_NAME, this, OUTPUT>,
  ): NAME extends keyof OUTPUT
    ? {
        error: `Naming conflict: "${NAME}" already exists in ${OUTPUT["name"]}`;
        suggestion: `Use .pipe("$${NAME}", transformer) or rename the stream`;
        conflictingProperty: OUTPUT[NAME];
      }
    : OUTPUT & { [K in NAME | (string & {})]: this };
  pipe<CUSTOM_NAME extends string, OUTPUT extends Stream<any, CUSTOM_NAME>>(
    transformer: Stream.Transformer<CUSTOM_NAME, this, OUTPUT>,
    name: CUSTOM_NAME,
  ): NAME extends keyof OUTPUT
    ? {
        error: `Naming conflict: "${NAME}" already exists in ${OUTPUT["name"]}`;
        suggestion: `Use .pipe("$${NAME}", transformer) or rename the stream`;
        conflictingProperty: OUTPUT[NAME];
      }
    : OUTPUT & { [K in NAME | (string & {})]: this };
  pipe<CUSTOM_NAME extends string, OUTPUT extends Stream<any, CUSTOM_NAME>>(
    name: CUSTOM_NAME,
    transformer: Stream.Transformer<CUSTOM_NAME, this, OUTPUT>,
  ): NAME extends keyof OUTPUT
    ? {
        error: `Naming conflict: "${NAME}" already exists in ${OUTPUT["name"]}`;
        suggestion: `Use .pipe("$${NAME}", transformer) or rename the stream`;
        conflictingProperty: OUTPUT[NAME];
      }
    : OUTPUT & { [K in NAME | (string & {})]: this };
  pipe(transformerOrName1: Function | string, transformerOrName2?: Function | string) {
    const output =
      typeof transformerOrName1 === "string" && typeof transformerOrName2 === "function"
        ? (transformerOrName2!(this, transformerOrName1) as any)
        : typeof transformerOrName1 === "function"
          ? (transformerOrName1!(this, transformerOrName2) as any)
          : void 0;

    if (this._name in output)
      throw new Error(
        `Naming conflict: Cannot name ${this.constructor.name} transformer with "${this._name}" ` +
          `because ${output.constructor.name} already has a property with that name.\n` +
          `Solutions:\n` +
          `  1. Use different name in pipe: .pipe("$${this._name}", ${this.constructor.name})\n` +
          `  2. Rename on stream creation: new Stream<T, "$${this._name}">()\n` +
          `conflictingProperty:${output[this._name]};`,
      );
    output[this._name] = this;

    const self = this;
    return new Proxy(output, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return target["root"] || self;
      },
    });
  }
  static generator<VALUE>(_source: Stream.Source<VALUE>): AsyncGenerator<VALUE, void, any> {
    return (async function* () {
      if (!_source) return;
      if (Symbol.asyncIterator in _source || Symbol.iterator in _source) {
        yield* _source;
      } else if (typeof _source === "function") {
        yield* _source();
      }
    })();
  }
}

export namespace Stream {
  export type ValueOf<T extends Stream<any, any>> = T extends Stream<infer VALUE, any> ? VALUE : never;
  export type NameOf<T extends Stream<any, any>> = T extends Stream<any, infer NAME> ? NAME : never;
  export type GeneratorFunction<VALUE> = () => AsyncGenerator<VALUE> | Generator<VALUE>;
  export type Source<VALUE> =
    | GeneratorFunction<VALUE>
    | AsyncIterable<VALUE>
    | Exclude<Iterable<VALUE>, string | String>;
  export type Transformer<NAME extends string, INPUT extends Stream<any, any>, OUTPUT extends Stream<any, NAME>> = (
    stream: INPUT,
    name?: NAME,
  ) => OUTPUT;
}

export class Controller extends Stream<void> {
  protected _aborted = false;
  protected _signals: Set<Stream<any>> | undefined;
  protected _cleanups: Set<Controller.Cleanup> | undefined;

  constructor(cleanup?: Controller.Cleanup) {
    super();
    if (cleanup) this.addCleanup(cleanup);
  }
  get aborted() {
    return this._aborted;
  }
  get signals() {
    return this._signals && [...this._signals];
  }
  async abort() {
    if (this._aborted) return;
    this._aborted = true;
    this._signals = undefined;

    for (const cleanup of this._cleanups ?? []) {
      cleanup.call(this);
    }
    this._cleanups = undefined;
    this.push();
    await new Promise((r) => setTimeout(r));
  }
  addCleanup(cleanup: Controller.Cleanup) {
    this._cleanups ? this._cleanups.add(cleanup) : (this._cleanups = new Set([cleanup]));
    return this;
  }
  removeCleanup(cleanup: Controller.Cleanup) {
    this._cleanups?.delete(cleanup);
    if (!this._cleanups?.size) this._cleanups = undefined;
    return this;
  }
  addSignal(signal: Stream<any>) {
    this._signals ? this._signals.add(signal) : (this._signals = new Set([signal]));

    signal.next().then(() => {
      if (this._signals?.has(signal)) this.abort();
    });

    return this;
  }
  removeSignal(signal: Stream<any>) {
    this._signals?.delete(signal);
    if (!this._signals?.size) this._signals = undefined;

    return this;
  }

  [Symbol.dispose](): void {
    this.abort();
  }
  static abort(controllers: Controller[]) {
    controllers.forEach((controller) => controller.abort());
  }
  static ABORTED = Symbol("aborted");
}
export namespace Controller {
  export type Cleanup = (this: Controller) => void | Promise<void>;
  export type Aborted = typeof Controller.ABORTED;
}
