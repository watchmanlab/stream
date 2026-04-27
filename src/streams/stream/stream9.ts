const NAME = "stream";

export class Stream<VALUE, NAME extends string = Stream.Name> implements AsyncIterable<VALUE>, Disposable {
  private consumers: Stream.Consumer<VALUE>[] = [];
  readonly name: NAME;
  private sourceConsumer?: SourceConsumer<VALUE>;
  constructor();
  constructor(name: NAME);
  constructor(source: Stream.Source<VALUE>);
  constructor(name: NAME, source: Stream.Source<VALUE>);
  constructor(nameOrSource?: NAME | Stream.Source<VALUE>, _source?: Stream.Source<VALUE>) {
    let source: Stream.Source<VALUE> | undefined;
    if (typeof nameOrSource === "string") {
      this.name = nameOrSource;
      source = _source;
    } else {
      this.name = NAME as NAME;
      source = nameOrSource;
    }

    if (source)
      this.sourceConsumer = new SourceConsumer(
        source,
        (value) => this.push(value),
        () => (this.sourceConsumer = undefined),
      );
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
      // resolve = undefined;
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
            this.sourceConsumer?.requestNext();
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
    this.clear();
    this.sourceConsumer?.terminate();
  }
  clear() {
    this.consumers.length = 0;
  }
}

class SourceConsumer<VALUE> {
  private iterator: Iterator<VALUE> | AsyncIterator<VALUE>;
  private requesting = false;

  constructor(
    source: Stream.Source<VALUE>,
    private onNext: (value: VALUE) => void,
    private onDone: () => void,
  ) {
    if (typeof source === "function") {
      this.iterator = source();
    } else {
      this.iterator = (source as any)[Symbol.asyncIterator]?.() ?? (source as any)[Symbol.iterator]();
    }
  }
  requestNext() {
    if (this.requesting) return;
    //
    this.requesting = true;

    const result = this.iterator.next();
    if (result instanceof Promise) {
      result.then((result) => {
        if (result.done) {
          this.onDone();
        } else {
          this.onNext(result.value);
        }
        this.requesting = false;
      });
    } else {
      if (result.done) {
        this.onDone();
      } else {
        this.onNext(result.value);
      }
      this.requesting = false;
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
  export type AnyStream = Stream<any, any>;
  export type AnyTransformer = Transformer<AnyStream, AnyStream>;
  export type AnyError = Error<any>;
  export type ExtractValue<T extends AnyStream> = T extends Stream<infer VALUE, any> ? VALUE : never;
  export type ExtractName<T extends AnyStream> = T extends Stream<any, infer NAME> ? NAME : never;
  export type ExtractError<T extends AnyError> = T extends Error<infer ERROR> ? ERROR : never;
  export type Source<VALUE> =
    | (() => AsyncGenerator<VALUE> | Generator<VALUE>)
    | AsyncIterable<VALUE>
    | Exclude<Iterable<VALUE>, string>;

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
