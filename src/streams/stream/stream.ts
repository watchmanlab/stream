const NAME = "stream";

export class Stream<VALUE, ERROR = unknown, NAME extends string = Stream.Name>
  implements AsyncIterable<VALUE>, Disposable
{
  private consumers: Stream.Consumer<VALUE>[] = [];

  readonly name: NAME;
  private _source?: Source<VALUE, ERROR, NAME, this>;
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
    this.consumers.push(consumer);

    try {
      while (true) {
        if (head) {
          yield head.value;
          head = head.next;
          if (!head) tail = undefined;
        } else {
          await new Promise<void>((r) => {
            resolve = r;
            this.source?.requestNext();
          });
        }
      }
    } finally {
      head = tail = undefined;
      resolve?.();
      resolve = undefined;
      const index = this.consumers.indexOf(consumer);
      if (index === -1) return;
      this.consumers.splice(index, 1);
    }
  }
  [Symbol.dispose]() {
    this.clear();
  }
  push(value: VALUE) {
    const consumers = this.consumers;
    const length = consumers.length;

    for (let i = 0; i < length; i++) {
      consumers[i](value);
    }
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
  terminate() {
    this.clear();
    this.source?.terminate();
  }
  clear() {
    this.consumers.length = 0;
  }
}

class Source<VALUE, ERROR, NAME extends string, STREAM extends Stream<VALUE, ERROR, NAME>> {
  private iterator: Iterator<VALUE | Stream.Error<ERROR>> | AsyncIterator<VALUE | Stream.Error<ERROR>>;
  private requesting = false;
  private _error?: Stream<Stream.SourceError<ERROR, STREAM>, never, `${NAME}Error`>;

  constructor(
    private stream: STREAM,
    source: Stream.SourceData<VALUE, ERROR>,
    private onDone: () => void,
  ) {
    if (typeof source === "function") {
      this.iterator = source();
    } else {
      this.iterator = (source as any)[Symbol.asyncIterator]?.() ?? (source as any)[Symbol.iterator]();
    }
  }
  get error() {
    if (!this._error) this._error = new Stream(`${this.stream.name}Error`);
    return this._error;
  }
  private async asyncResult(resultPromise: Promise<IteratorResult<VALUE | Stream.Error<ERROR>, any>>) {
    try {
      const result = await resultPromise;
      if (result.done) {
        this.onDone();
      } else if (result.value instanceof Stream.Error) {
        if (!this._error) throw result.value;
        this._error?.push(new Stream.SourceError(result.value.value, this.stream));
      } else {
        this.stream.push(result.value);
      }

      this.requesting = false;
    } catch (error: any) {
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
    if (result.done) {
      this.onDone();
    } else if (result.value instanceof Stream.Error) {
      if (!this._error) throw result.value;
      this._error?.push(new Stream.SourceError(result.value.value, this.stream));
    } else {
      this.stream.push(result.value);
    }

    this.requesting = false;
  }
  requestNext() {
    if (this.requesting) return;

    this.requesting = true;

    let result:
      | IteratorResult<VALUE | Stream.Error<ERROR>, any>
      | Promise<IteratorResult<VALUE | Stream.Error<ERROR>, any>>;
    try {
      result = this.iterator.next();
    } catch (error: any) {
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
    this.iterator.return?.();
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
  export class Error<ERROR> {
    constructor(public readonly value: ERROR) {}
  }
  export const EMPTY = Symbol("$EMPTY#");
  export type Empty = typeof EMPTY;
}

function simpleTest() {
  const stream = new Stream([1, 2, 3]);

  (async () => {
    for await (const value of stream) {
      console.log(value);
    }

    console.log("abort");
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
newStreamBench();

// async function* test(i: number) {
//   if (i == 1) throw "kechmahaja";
//   await new Promise((r) => setTimeout(r, 10));
//   yield 1;
//   await new Promise((r) => setTimeout(r, 10));
//   if (i == 2) throw new Error("kechmahaja2222");
//   yield 2;
// }

// const gen = test(2);
// const p1 = gen.next();
// const p2 = gen.next();
// try {
//   await p1;
//   await p2;
// } catch (error) {}
