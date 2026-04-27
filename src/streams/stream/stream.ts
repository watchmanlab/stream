const NAME = "root";

export class Stream<VALUE, ERROR = unknown, NAME extends string = Stream.Name>
  implements AsyncIterable<VALUE>, Disposable
{
  private _consumers: Stream.Consumer<VALUE>[] = [];
  private _source?: Source<VALUE, ERROR, NAME, this>;
  readonly name: NAME;
  constructor();
  constructor(name: NAME);
  constructor(sourceData: Stream.SourceData<VALUE, ERROR>);
  constructor(name: NAME, sourceData: Stream.SourceData<VALUE, ERROR>);
  constructor(nameOrSourceData?: NAME | Stream.SourceData<VALUE, ERROR>, sourceData?: Stream.SourceData<VALUE, ERROR>) {
    let source: Stream.SourceData<VALUE, ERROR> | undefined;
    if (typeof nameOrSourceData === "string") {
      this.name = nameOrSourceData;
      source = sourceData;
    } else {
      this.name = NAME as NAME;
      source = nameOrSourceData;
    }

    if (source) this._source = new Source(this, source, () => (this._source = undefined));
  }
  get source() {
    return this._source;
  }
  async *[Symbol.asyncIterator]() {
    let resolve: (() => void) | undefined;

    let head: { value: VALUE; next?: typeof head } | undefined;
    let tail: typeof head;

    const consumer: Stream.Consumer<VALUE> = (value: VALUE) => {
      const node = { value };
      if (!head) {
        head = tail = node;
      } else {
        tail!.next = node;
        tail = node;
      }
      resolve?.();
    };
    this._consumers.push(consumer);

    try {
      while (true) {
        if (head) {
          if (head.value === Stream.TERMINATE) break;
          yield head.value;
          head = head.next;
          if (!head) tail = undefined;
        } else {
          await new Promise<void>((r) => {
            resolve = r;
            if (this._source?.idle) this._source?.requestNext();
          });
        }
      }
    } finally {
      head = tail = undefined;
      resolve?.();
      resolve = undefined;

      if (!this._consumers.length) return;
      const index = this._consumers.indexOf(consumer);
      if (index === -1) return;
      this._consumers.splice(index, 1);
    }
  }
  [Symbol.dispose]() {
    this.terminate();
  }
  push(value: VALUE) {
    const _consumers = this._consumers;
    const length = _consumers.length;

    for (let i = 0; i < length; i++) {
      _consumers[i](value);
    }
  }
  next() {
    return this[Symbol.asyncIterator]().next();
  }
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream<any, any, OUTPUT_NAME>>(
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): Stream.Transformer<OUTPUT_STREAM, this>;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream<any, any, OUTPUT_NAME>>(
    name: OUTPUT_NAME,
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): Stream.Transformer<OUTPUT_STREAM, this>;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream<any, any, OUTPUT_NAME>>(
    nameOrTransform: OUTPUT_NAME | Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
    transform?: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): Stream.Transformer<OUTPUT_STREAM, this> {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }
  terminate(terminateSource = true) {
    this._consumers.length = 0;
    this.push(Stream.TERMINATE as never);
    if (terminateSource) this.source?.terminate();
  }
}

class Source<VALUE, ERROR, NAME extends string, STREAM extends Stream<VALUE, ERROR, NAME>> {
  private _iterator: Iterator<VALUE | Stream.Error<ERROR>> | AsyncIterator<VALUE | Stream.Error<ERROR>>;
  private _idle = true;
  private _error?: Stream<Stream.SourceError<ERROR, STREAM>, never, `${NAME}Error`>;

  constructor(
    private stream: STREAM,
    source: Stream.SourceData<VALUE, ERROR>,
    private onDone: () => void,
  ) {
    if (typeof source === "function") {
      this._iterator = source();
    } else {
      this._iterator = (source as any)[Symbol.asyncIterator]?.() ?? (source as any)[Symbol.iterator]();
    }
  }
  get idle() {
    return this._idle;
  }
  get error() {
    if (!this._error) this._error = new Stream(`${this.stream.name}Error`);
    return this._error;
  }
  private async asyncResult(resultPromise: Promise<IteratorResult<VALUE | Stream.Error<ERROR>, any>>) {
    try {
      const result = await resultPromise;
      this._idle = true;
      if (result.done) {
        this.onDone();
      } else if (result.value instanceof Stream.Error) {
        if (!this._error) throw result.value;
        this._error?.push(new Stream.SourceError(result.value.value, this.stream));
      } else {
        this.stream.push(result.value);
      }
    } catch (error: any) {
      this._idle = true;
      if (error instanceof Stream.Error) {
        if (!this._error) throw error.value;
        this._error?.push(new Stream.SourceError(error.value, this.stream));
      } else {
        if (!this._error) throw error;
        this._error?.push(new Stream.SourceError(error, this.stream));
      }
    }
  }
  private syncResult(result: IteratorResult<VALUE | Stream.Error<ERROR>, any>) {
    this._idle = true;
    if (result.done) {
      this.onDone();
    } else if (result.value instanceof Stream.Error) {
      if (!this._error) throw result.value;
      this._error?.push(new Stream.SourceError(result.value.value, this.stream));
    } else {
      this.stream.push(result.value);
    }
  }
  requestNext() {
    this._idle = false;

    let result:
      | IteratorResult<VALUE | Stream.Error<ERROR>, any>
      | Promise<IteratorResult<VALUE | Stream.Error<ERROR>, any>>;
    try {
      result = this._iterator.next();
    } catch (error: any) {
      this._idle = true;
      if (error instanceof Stream.Error) {
        if (!this._error) throw error.value;
        this._error?.push(new Stream.SourceError(error.value, this.stream));
      } else {
        if (!this._error) throw error;
        this._error?.push(new Stream.SourceError(error, this.stream));
      }
      return;
    }
    if (result instanceof Promise) {
      this.asyncResult(result);
    } else {
      this.syncResult(result);
    }
  }
  terminate() {
    this._iterator.return?.();
    this.onDone();
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type Consumer<VALUE> = (value: VALUE) => void;
  export type AnyStream = Stream<any, any, any>;
  export type AnyTransformer = Transformer<AnyStream, AnyStream>;
  export type AnyError = Error<any>;
  export type ExtractValue<T extends AnyStream> = T extends Stream<infer VALUE, any, any> ? VALUE : never;
  export type ExtractName<T extends AnyStream> = T extends Stream<any, any, infer NAME> ? NAME : never;
  export type ExtractError<T extends AnyError | AnyStream> =
    T extends Error<infer ERROR> ? ERROR : T extends Stream<any, infer ERROR, any> ? ERROR : never;
  export type SourceData<VALUE, ERROR> =
    | (() => AsyncGenerator<VALUE | Error<ERROR>> | Generator<VALUE | Error<ERROR>>)
    | AsyncIterable<VALUE | Error<ERROR>>
    | Exclude<Iterable<VALUE | Error<ERROR>>, string>;

  export type Transform<
    INPUT_STREAM extends AnyStream,
    OUTPUT_NAME extends string,
    OUTPUT_STREAM extends Stream<any, any, OUTPUT_NAME>,
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
  export class SourceError<ERROR, SOURCE extends AnyStream> {
    constructor(
      public readonly error: ERROR,
      public readonly source: SOURCE,
    ) {}
    get sourceName(): SOURCE["name"] {
      return this.source.name;
    }
  }
  export class TransformerError<VALUE, ERROR, SOURCE extends AnyStream> extends SourceError<ERROR, SOURCE> {
    constructor(
      public readonly value: VALUE,
      error: ERROR,
      source: SOURCE,
    ) {
      super(error, source);
    }
  }
  export class Error<const ERROR> {
    constructor(public readonly value: ERROR) {}
  }

  export const TERMINATE = Symbol("$TERMINATE#");
  export type Terminate = typeof TERMINATE;
}

function simpleTest() {
  const stream = new Stream(async function* () {
    await new Promise((r) => setTimeout(r, 10));
    yield 1;
    await new Promise((r) => setTimeout(r, 10));
    yield 2;
    await new Promise((r) => setTimeout(r, 10));
    yield 3;
  });

  (async () => {
    for await (const value of stream) {
      console.log("c1", value);
      if (value == 2) break;
    }
  })();
  (async () => {
    for await (const value of stream) {
      console.log("c2", value);
      if (value == 2) break;
    }
  })();

  // stream.push(44);
  // stream.push(55);
}
function newStreamBench() {
  const MAX = 1_000_000;
  const now = performance.now();

  const stream = new Stream<number>();

  (async () => {
    for await (const value of stream) {
      let result = value + 10;
      if (result === 40010) {
        result = 444;
      } else {
        result = 555;
      }
      if (value === MAX) {
        console.log("new stream", Math.round(performance.now() - now));
        return;
      }
    }
  })();

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}

// simpleTest();
// newStreamBench();
