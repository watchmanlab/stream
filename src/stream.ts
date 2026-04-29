const NAME = "root";

export class Stream<VALUE, ERROR, NAME extends string = Stream.Name> implements AsyncIterable<VALUE>, Disposable {
  private _consumers: Consumers<VALUE, NAME>;
  private _source?: Source<VALUE, ERROR, NAME>;
  readonly name: NAME;
  constructor();
  constructor(name: NAME);
  constructor(sourceData: Stream.SourceData<VALUE, ERROR>);
  constructor(name: NAME, sourceData: Stream.SourceData<VALUE, ERROR>);
  constructor(
    nameOrSourceData?: NAME | Stream.SourceData<VALUE, ERROR>,
    _sourceData?: Stream.SourceData<VALUE, ERROR>,
  ) {
    let sourceData: Stream.SourceData<VALUE, ERROR> | undefined;
    if (typeof nameOrSourceData === "string") {
      this.name = nameOrSourceData;
      sourceData = _sourceData;
    } else {
      this.name = NAME as NAME;
      sourceData = nameOrSourceData;
    }

    if (sourceData)
      this._source = new Source({
        name: this.name,
        sourceData,
        onNext: (value) => this.push(value),
        onDone: () => (this._source = undefined),
      });

    this._consumers = new Consumers(this.name);
  }
  get consumers() {
    return this._consumers;
  }
  get source() {
    return this._source;
  }
  async *[Symbol.asyncIterator]() {
    let resolve: (() => void) | undefined;

    let head: { value: VALUE; next?: typeof head } | undefined;
    let tail: typeof head;

    const consumer = this._consumers.add((value: VALUE) => {
      const node = { value };
      if (!head) {
        head = tail = node;
      } else {
        tail!.next = node;
        tail = node;
      }
      resolve?.();
    });

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

      this._consumers.remove(consumer);
    }
  }
  [Symbol.dispose]() {
    this.terminate();
  }
  push(value: VALUE) {
    const consumers = this._consumers;
    const length = consumers.count;

    for (let i = 0; i < length; i++) {
      consumers[i](value);
    }
  }
  next() {
    return this[Symbol.asyncIterator]().next();
  }
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream.Transformer<this, any, any, OUTPUT_NAME>>(
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream.Transformer<this, any, any, OUTPUT_NAME>>(
    name: OUTPUT_NAME,
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream.Transformer<this, any, any, OUTPUT_NAME>>(
    nameOrTransform: OUTPUT_NAME | Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
    transform?: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }
  terminate(terminateSource = true) {
    this._consumers.clear();
    this.push(Stream.TERMINATE as never);
    if (terminateSource) this._source?.terminate();
  }
}

class Queue<VALUE> implements Iterable<VALUE> {
  private _head?: Queue.Node<VALUE>;
  private _tail?: Queue.Node<VALUE>;
  private _size = 0;

  get size() {
    return this._size;
  }
  enqueue(value: VALUE): void {
    this._size++;
    const node = { value };
    if (!this._head) {
      this._head = this._tail = node;
    } else {
      this._tail!.next = node;
      this._tail = node;
    }
  }
  dequeue(): VALUE | undefined {
    if (!this._head) return;
    this._size--;
    const value = this._head.value;
    this._head = this._head.next;

    return value;
  }
  *[Symbol.iterator]() {
    let current = this._head;
    while (current) {
      yield current.value;
      current = current.next;
    }
  }
}
export namespace Queue {
  export type Node<VALUE> = { value: VALUE; next?: Node<VALUE> } | undefined;
}
interface Consumers<VALUE, NAME extends string> {
  [index: number]: Stream.Consumer<VALUE>;
}

class Consumer<VALUE> {
  private _queue = new Queue<VALUE>();

  constructor(public readonly fn: (value: VALUE) => void) {}
  push(value: VALUE) {

    ,,,,,,,,,,
  }
  get queue() {
    return this._queue;
  }
}
class Consumers<VALUE, NAME extends string> implements Iterable<Stream.Consumer<VALUE>> {
  private _list: Stream.Consumer<VALUE>[] = [];
  private _added?: Stream<Stream.Consumer<VALUE>, never, `${NAME}ConsumerAdded`>;
  private _removed?: Stream<Stream.Consumer<VALUE>, never, `${NAME}ConsumerRemoved`>;
  private _cleared?: Stream<void, never, `${NAME}ConsumersCleared`>;

  constructor(private name: NAME) {
    return new Proxy(this, {
      get(target, prop: any, receiver) {
        if (!isNaN(prop)) {
          return target._list[prop];
        }
        return Reflect.get(target, prop, receiver);
      },
      set(target, prop: any, value) {
        if (!isNaN(prop)) {
          target._list[prop] = value;
          return true;
        }
        return Reflect.set(target, prop, value);
      },
    });
  }
  add(consumer: Stream.Consumer<VALUE>): Stream.Consumer<VALUE> {
    this._list.push(consumer);
    this._added?.push(consumer);
    return consumer;
  }
  remove(consumer: Stream.Consumer<VALUE>): void {
    if (!this._list.length) return;
    const index = this._list.indexOf(consumer);
    if (index === -1) return;
    this._list.splice(index, 1);
    this._removed?.push(consumer);
  }
  clear() {
    this._list.length = 0;
    this._cleared?.push();
  }
  [Symbol.iterator]() {
    return this._list[Symbol.iterator]();
  }

  get count() {
    return this._list.length;
  }
  get added() {
    if (!this._added) {
      this._added = new Stream(`${this.name}ConsumerAdded`);
    }
    return this._added;
  }
  get removed() {
    if (!this._removed) {
      this._removed = new Stream(`${this.name}ConsumerRemoved`);
    }
    return this._removed;
  }
  get cleared() {
    if (!this._cleared) {
      this._cleared = new Stream(`${this.name}ConsumersCleared`);
    }
    return this._cleared;
  }
}
class Source<VALUE, ERROR, NAME extends string> {
  private _iterator: Iterator<VALUE | Stream.Error<ERROR>> | AsyncIterator<VALUE | Stream.Error<ERROR>>;
  private _idle = true;
  private _error?: Stream<ERROR, never, `${NAME}Error`>;

  constructor(
    private options: {
      name: NAME;
      sourceData: Stream.SourceData<VALUE, ERROR>;
      onNext: (value: VALUE) => void;
      onDone: () => void;
    },
  ) {
    if (typeof options.sourceData === "function") {
      this._iterator = options.sourceData();
    } else {
      this._iterator =
        (options.sourceData as any)[Symbol.asyncIterator]?.() ?? (options.sourceData as any)[Symbol.iterator]();
    }
  }
  get idle() {
    return this._idle;
  }
  get error() {
    if (!this._error) this._error = new Stream(`${this.options.name}Error`);
    return this._error;
  }
  private async asyncResult(resultPromise: Promise<IteratorResult<VALUE | Stream.Error<ERROR>, any>>) {
    try {
      const result = await resultPromise;
      this._idle = true;
      if (result.done) {
        this.options.onDone();
      } else if (result.value instanceof Stream.Error) {
        if (!this._error) throw result.value;
        this._error?.push(result.value.data);
      } else {
        this.options.onNext(result.value);
      }
    } catch (error: any) {
      this._idle = true;
      if (error instanceof Stream.Error) {
        if (!this._error) throw error.data;
        this._error?.push(error.data);
      } else {
        if (!this._error) throw error;
        this._error?.push(error);
      }
    }
  }
  private syncResult(result: IteratorResult<VALUE | Stream.Error<ERROR>, any>) {
    this._idle = true;
    if (result.done) {
      this.options.onDone();
    } else if (result.value instanceof Stream.Error) {
      if (!this._error) throw result.value;
      this._error?.push(result.value.data);
    } else {
      this.options.onNext(result.value);
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
        if (!this._error) throw error.data;
        this._error?.push(error.data);
      } else {
        if (!this._error) throw error;
        this._error?.push(error);
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
    (this, this.options.onDone());
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type AnyStream = Stream<any, any, any>;
  export type AnyTransformer = Transformer<AnyStream, any, any, any>;
  export type AnyError = Error<any>;
  export type AnyConsumer = Consumer<any>;
  export type ExtractValue<T extends AnyStream | AnyTransformer> =
    T extends Stream<infer VALUE, any, any> ? VALUE : T extends Transformer<any, infer VALUE, any, any> ? VALUE : never;

  export type ExtractError<T extends AnyError | AnyStream | AnyTransformer> =
    T extends Error<infer ERROR>
      ? ERROR
      : T extends Stream<any, infer ERROR, any>
        ? ERROR
        : T extends Transformer<any, any, infer ERROR, any>
          ? ERROR
          : never;
  export type Consumer<VALUE> = (value: VALUE) => void;
  export type SourceData<VALUE, ERROR> =
    | (() => AsyncGenerator<VALUE | Error<ERROR>> | Generator<VALUE | Error<ERROR>>)
    | AsyncIterable<VALUE | Error<ERROR>>
    | Exclude<Iterable<VALUE | Error<ERROR>>, string>;

  export type Transform<
    INPUT_STREAM extends AnyStream,
    OUTPUT_NAME extends string,
    OUTPUT_STREAM extends Transformer<INPUT_STREAM, any, any, OUTPUT_NAME>,
  > = (inputStream: INPUT_STREAM, name?: OUTPUT_NAME) => OUTPUT_STREAM;

  export type ExtractInputStream<T extends AnyStream> =
    T extends Transformer<infer INPUT_STREAM, any, any, any> ? INPUT_STREAM : never;
  export type Traversable<T extends AnyStream> =
    ExtractInputStream<T> extends never
      ? T
      : Omit<T, "traversal"> &
          Record<ExtractInputStream<T>["name"] | (`$${string}` & {}), Traversable<ExtractInputStream<T>>>;
  export abstract class Transformer<
    INPUT_STREAM extends Stream.AnyStream,
    VALUE,
    ERROR extends { error: unknown; reason: ExtractValue<INPUT_STREAM> },
    NAME extends string,
  > extends Stream<VALUE, ERROR, NAME> {
    constructor(
      name: NAME,
      protected readonly inputStream: INPUT_STREAM,
      fn?: () => AsyncGenerator<VALUE | Error<ERROR>>,
    ) {
      super(name, fn!);

      return new Proxy(this, {
        get(target, p, receiver) {
          if (p in target) return Reflect.get(target, p, receiver);
          return inputStream;
        },
      });
    }
    get traversal(): Record<INPUT_STREAM["name"] | (`$${string}` & {}), Traversable<INPUT_STREAM>> {
      const self = this;
      return new Proxy(
        {},
        {
          get() {
            return self.inputStream;
          },
        },
      ) as never;
    }
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

  export class Error<const ERROR> {
    constructor(public readonly data: ERROR) {}
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

  const stream = new Stream<number, never>();

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

simpleTest();
// newStreamBench();
