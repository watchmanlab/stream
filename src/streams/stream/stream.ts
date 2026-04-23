import { Stream as OldStream } from "./stream0";

const NAME = "stream";
export class Stream<VALUE, NAME extends string = Stream.Name> implements AsyncIterable<VALUE>, Disposable {
  private _consumers: Stream.Consumer<VALUE>[] = [];
  private _lifecycles: {
    consumerAdded?: Stream<Stream.Consumer<VALUE>, `${NAME}ConsumerAdded`>;
    firstConsumerAdded?: Stream<Stream.Consumer<VALUE>, `${NAME}FirstConsumerAdded`>;
    consumerRemoved?: Stream<Stream.Consumer<VALUE>, `${NAME}ConsumerRemoved`>;
    lastConsumerRemoved?: Stream<Stream.Consumer<VALUE>, `${NAME}LastConsumerRemoved`>;
    valueDropped?: Stream<VALUE, `${NAME}ValueDropped`>;
    terminated?: Stream<void, `${NAME}Terminated`>;
    cleared?: Stream<void, `${NAME}Cleared`>;
    isTerminated?: true;
  } = {};

  constructor(public readonly name = NAME as NAME) {}

  get consumerAdded() {
    if (!this._lifecycles.consumerAdded) this._lifecycles.consumerAdded = new Stream(`${this.name}ConsumerAdded`);
    return this._lifecycles.consumerAdded;
  }
  get firstConsumerAdded() {
    if (!this._lifecycles.firstConsumerAdded)
      this._lifecycles.firstConsumerAdded = new Stream(`${this.name}FirstConsumerAdded`);
    return this._lifecycles.firstConsumerAdded;
  }
  get consumerRemoved() {
    if (!this._lifecycles.consumerRemoved) this._lifecycles.consumerRemoved = new Stream(`${this.name}ConsumerRemoved`);
    return this._lifecycles.consumerRemoved;
  }
  get lastConsumerRemoved() {
    if (!this._lifecycles.lastConsumerRemoved)
      this._lifecycles.lastConsumerRemoved = new Stream(`${this.name}LastConsumerRemoved`);
    return this._lifecycles.lastConsumerRemoved;
  }
  get valueDropped() {
    if (!this._lifecycles.valueDropped) this._lifecycles.valueDropped = new Stream(`${this.name}ValueDropped`);
    return this._lifecycles.valueDropped;
  }
  get terminated() {
    if (!this._lifecycles.terminated) this._lifecycles.terminated = new Stream(`${this.name}Terminated`);
    return this._lifecycles.terminated;
  }
  get cleared() {
    if (!this._lifecycles.cleared) this._lifecycles.cleared = new Stream(`${this.name}Cleared`);
    return this._lifecycles.cleared;
  }
  get isTerminated() {
    return (this._lifecycles.isTerminated = true);
  }
  get consumersCount() {
    return this._consumers.length;
  }
  get consumers() {
    return this._consumers.values();
  }

  [Symbol.dispose]() {
    this.terminate();
  }
  async *[Symbol.asyncIterator]() {
    let resolve: (value: VALUE) => void;
    let promise = new Promise<VALUE>((r) => (resolve = r));

    const { abort, ready } = this.listen((value) => {
      resolve?.(value);
    });

    try {
      while (true) {
        const value = await promise;
        promise = new Promise<VALUE>((r) => (resolve = r));
        ready();
        yield value;
      }
    } finally {
      abort();
    }
  }
  push(value: VALUE) {
    const consumers = this._consumers;
    const length = consumers.length;
    if (!length) {
      this._lifecycles.valueDropped?.push(value);
      return;
    }
    for (let i = 0; i < length; i++) {
      const consumer = consumers[i];
      if (consumer.isReady) {
        consumer.isReady = false;
        consumer.fn(value, consumer);
      } else {
        consumer.buffer.push(value);
      }
    }
  }

  listen(fn: Stream.Fn<VALUE>, abortSignal?: Stream.AnyStream): Stream.Consumer<VALUE> {
    if (this._lifecycles.isTerminated) throw new Error(`stream ${this.name} is terminated`);
    abortSignal?.listenOnce(abort);

    const buffer: VALUE[] = [];
    const consumer: Stream.Consumer<VALUE> = {
      ready,
      buffer,
      isReady: true,
      abort,
      fn,
      [Symbol.dispose]: abort,
    };
    const consumers = this._consumers;
    const lifecycles = this._lifecycles;

    consumers.push(consumer);

    lifecycles.consumerAdded?.push(consumer);

    if (this._consumers.length === 1) lifecycles.firstConsumerAdded?.push(consumer);

    return consumer;
    function ready() {
      consumer.isReady = true;
      const value = buffer.shift();
      if (!value) return;

      consumer.isReady = false;
      fn(value, consumer);
    }
    function abort() {
      buffer.length = 0;

      const index = consumers.indexOf(consumer);
      if (index === -1) return;

      consumers.splice(index, 1);

      lifecycles.consumerRemoved?.push(consumer);

      if (!consumers.length) lifecycles.lastConsumerRemoved?.push(consumer);
    }
  }
  listenOnce(fn: Stream.Fn<VALUE>): Stream.Consumer<VALUE> {
    return this.listen((value, consumer) => {
      fn(value, consumer);
      consumer.abort();
    });
  }
  nextOrThrow(abortSignal?: Stream.AnyStream): Promise<VALUE> {
    return new Promise<VALUE>((resolve, reject) => {
      this.listenOnce(resolve);
      this.terminated.listenOnce(reject);
      abortSignal?.listenOnce(reject);
    });
  }
  next(abortSignal?: Stream.AnyStream): Promise<VALUE | Stream.Empty> {
    return new Promise<VALUE | Stream.Empty>((resolve) => {
      this.listenOnce((value) => resolve(value));
      this.terminated.listenOnce(() => resolve(Stream.EMPTY));
      abortSignal?.listenOnce(() => resolve(Stream.EMPTY));
    });
  }
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream<any, OUTPUT_NAME>>(
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): Stream.Transformer<OUTPUT_STREAM, this>;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream<any, OUTPUT_NAME>>(
    name: OUTPUT_NAME,
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): Stream.Transformer<OUTPUT_STREAM, this>;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream<any, OUTPUT_NAME>>(
    nameOrTransform: OUTPUT_NAME | Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
    transform?: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): Stream.Transformer<OUTPUT_STREAM, this> {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }

  terminate() {
    if (this._lifecycles.isTerminated) return;

    this._consumers.length = 0;
    this._lifecycles.terminated?.push();
    Object.values(this._lifecycles).forEach((lifecycle) => lifecycle instanceof Stream && lifecycle.terminate());
    this._lifecycles = { isTerminated: true };
  }
  clear() {
    this._consumers.length = 0;
    this._lifecycles.cleared?.push();
    Object.values(this._lifecycles).forEach((lifecycle) => lifecycle instanceof Stream && lifecycle.clear());
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type Abort = () => void;
  export type Ready = () => void;
  export type Fn<VALUE> = (value: VALUE, consumer: Consumer<VALUE>) => void;
  export type Consumer<VALUE> = {
    ready: Ready;
    buffer: VALUE[];
    isReady: boolean;
    abort: Abort;
    fn: Fn<VALUE>;
  } & Disposable;

  export type AnyStream = Stream<any, any>;
  export type AnyTransformer = Transformer<AnyStream, AnyStream>;
  export type AnyError = Error<any>;
  export type ExtractValue<T extends AnyStream> = T extends Stream<infer VALUE, any> ? VALUE : never;
  export type ExtractName<T extends AnyStream> = T extends Stream<any, infer NAME> ? NAME : never;
  export type ExtractError<T extends AnyError> = T extends Error<infer ERROR> ? ERROR : never;

  export type Transform<
    INPUT_STREAM extends AnyStream,
    OUTPUT_NAME extends string,
    OUTPUT_STREAM extends Stream<any, OUTPUT_NAME>,
  > = (inputStream: INPUT_STREAM, name?: OUTPUT_NAME) => Transformer<OUTPUT_STREAM, INPUT_STREAM>;
  export type Transformer<OUTPUT_STREAM extends AnyStream, INPUT_STREAM extends AnyStream> = OUTPUT_STREAM &
    Record<ExtractName<INPUT_STREAM> | (`$${string}` & {}), INPUT_STREAM>;
  export function transformer<OUTPUT_STREAM extends AnyStream, INPUT_STREAM extends AnyStream>(
    outputStream: OUTPUT_STREAM,
    inputStream: INPUT_STREAM,
  ): Transformer<OUTPUT_STREAM, INPUT_STREAM> {
    return new Proxy(outputStream, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return inputStream;
      },
    }) as never;
  }
  export class SourceError<VALUE, ERROR, SOURCE extends AnyStream> {
    constructor(
      public readonly value: VALUE,
      public readonly error: ERROR,
      public readonly source: SOURCE,
    ) {}
    get sourceName(): SOURCE["name"] {
      return this.source.name;
    }
  }
  export class Error<ERROR> {
    constructor(public readonly value: ERROR) {}
  }
  export const EMPTY = Symbol("$EMPTY#");
  export type Empty = typeof EMPTY;
}

function simpleTest() {
  const stream = new Stream<number>();
  stream.listen(async (value, { ready, abort }) => {
    await new Promise((r) => setTimeout(r, Math.random() * 200));
    ready();

    console.log("listener", value);
  });

  // (async () => {
  //   for await (const value of stream) {
  //     console.log("generator", value);
  //   }

  //   console.log("abort");
  // })();

  (async () => {
    stream.push(1);
    stream.push(2);
    stream.push(3);
  })();
}
function newStreamBench() {
  const MAX = 1_000_000;
  const now = performance.now();

  const stream = new Stream<number>();
  stream.listen((value, { ready, abort }) => {
    // await new Promise((r) => setTimeout(r, Math.random() * 200));
    if (value === MAX) {
      console.log("new stream", Math.round(performance.now() - now));
      abort();
      return;
    }
    ready();
  });
  // (async () => {
  //   for await (const value of stream) {
  //     if (value === MAX) {
  //       console.log("new stream gen", Math.round(performance.now() - now));

  //       return;
  //     }
  //   }
  // })();

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}
function oldStreamBench() {
  const MAX = 1_000_000;

  const stream = new OldStream<number>();

  stream.listen((value) => {
    // await new Promise((r) => setTimeout(r, Math.random() * 200));
    if (value === MAX) console.log("old stream", Math.round(performance.now() - now));
  });

  const now = performance.now();
  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}

simpleTest();
// newStreamBench();
// oldStreamBench()
