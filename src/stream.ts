export class Stream<VALUE> implements AsyncIterable<VALUE> {
  protected consumers = new Map<VALUE[], () => void>();

  constructor(protected source?: Stream.Source<VALUE>) {}

  get consumersCount() {
    return this.consumers.size;
  }

  async push(value: VALUE, ...values: VALUE[]) {
    for (const [queue, resolver] of this.consumers) {
      queue.push(value, ...values);
      resolver();
    }
    await new Promise((r) => setTimeout(r));
  }
  protected getGenerator(): AsyncGenerator<VALUE, void, any> | undefined {
    const self = this;

    return (async function* () {
      if (!self.source) return;
      if (Symbol.asyncIterator in self.source || Symbol.iterator in self.source) {
        yield* self.source;
      } else if (typeof self.source === "function") {
        yield* self.source();
      }
    })();
  }
  protected requestingNext = false;

  async *[Symbol.asyncIterator]() {
    const generator = this.getGenerator();

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
            this.consumers.set(queue, resolve);
          });
        }
      }
    } finally {
      this.consumers.delete(queue);
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
  pipe<OUTPUT extends Stream<any>>(
    transformer: Stream.Transformer<this, OUTPUT>,
  ): Stream<Stream.ValueOf<OUTPUT>> & Prettify<Omit<this & OUTPUT, keyof Stream<any>>> {
    const output = transformer(this);

    for (const key in output) {
      if (baseProps.has(key)) continue;
      if (output.hasOwnProperty(key) && this.hasOwnProperty(key)) {
        throw new Error(
          `Capability override detected: "${key}" already exists. ` +
            `Use snapshot() to preserve multiple instances of the same capability transformer.`,
        );
      }
    }
    for (const key in this) {
      if (!(key in output)) {
        Object.defineProperty(output, key, Object.getOwnPropertyDescriptor(this, key)!);
      }
    }

    return output as never;
  }
  static create<VALUE, CAP extends Record<string, any>>(
    source: Stream.Source<VALUE>,
    getCapabilities: () => CAP,
  ): Stream.CapableStream<VALUE, CAP> {
    const output = new Stream<VALUE>(source) as Stream<VALUE> & CAP;

    const capabilities = getCapabilities();
    for (const key of Object.keys(capabilities)) {
      Object.defineProperty(output, key, {
        value: capabilities[key],
        enumerable: true,
        configurable: false,
      });
    }

    return output;
  }
}

export namespace Stream {
  export type ValueOf<STREAM> = STREAM extends Stream<infer VALUE> ? VALUE : never;
  export type GeneratorFunction<VALUE> = () => AsyncGenerator<VALUE> | Generator<VALUE>;
  export type Source<VALUE> = GeneratorFunction<VALUE> | AsyncIterable<VALUE> | Iterable<VALUE>;
  export type Transformer<INPUT extends Stream<any>, OUTPUT extends Stream<any> = INPUT> = (stream: INPUT) => OUTPUT;
  export type CapableStream<VALUE, CAP extends Record<string, any>> = Stream<VALUE> & CAP;
}

export function controller(): Stream.Transformer<
  Stream<any>,
  Stream.CapableStream<controller.Aborted, controller.ControllerCapability>
> {
  return function (source) {
    return Stream.create<controller.Aborted, controller.ControllerCapability>(
      async function* () {
        for await (const _ of source) {
          yield controller.ABORTED;
        }
      },
      () => {
        return { controller: { aborted: true } };
      },
    );
  };
}
export namespace controller {
  export const ABORTED = Symbol("aborted");
  export type Aborted = typeof ABORTED;
  export type Controller = { aborted: boolean };
  export type ControllerCapability = { controller: Controller };
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

type Prettify<T> = T extends object ? { [K in keyof T]: T[K] } : T;

const baseProps = new Set(Object.keys(new Stream()));
