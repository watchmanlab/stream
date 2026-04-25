const NAME = "stream";
export class Stream<VALUE, NAME extends string = Stream.Name> implements AsyncIterable<VALUE>, Disposable {
  private _listeners: Stream.listener<VALUE>[] = [];
  private _firstListener: Stream.listener<VALUE> = () => {};
  private _lifecycles: {
    listenerAdded?: Stream<Stream.listener<VALUE>, `${NAME}ListenerAdded`>;
    firstListenerAdded?: Stream<Stream.listener<VALUE>, `${NAME}FirstListenerAdded`>;
    listenerRemoved?: Stream<Stream.listener<VALUE>, `${NAME}ListenerRemoved`>;
    lastListenerRemoved?: Stream<Stream.listener<VALUE>, `${NAME}LastListenerRemoved`>;
    valueDropped?: Stream<VALUE, `${NAME}ValueDropped`>;
    terminated?: Stream<void, `${NAME}Terminated`>;
    cleared?: Stream<void, `${NAME}Cleared`>;
    isTerminated?: true;
  } = {};

  constructor(public readonly name = NAME as NAME) {}

  get listenerAdded() {
    if (!this._lifecycles.listenerAdded) this._lifecycles.listenerAdded = new Stream(`${this.name}ListenerAdded`);
    return this._lifecycles.listenerAdded;
  }
  get firstListenerAdded() {
    if (!this._lifecycles.firstListenerAdded)
      this._lifecycles.firstListenerAdded = new Stream(`${this.name}FirstListenerAdded`);
    return this._lifecycles.firstListenerAdded;
  }
  get listenerRemoved() {
    if (!this._lifecycles.listenerRemoved) this._lifecycles.listenerRemoved = new Stream(`${this.name}ListenerRemoved`);
    return this._lifecycles.listenerRemoved;
  }
  get lastListenerRemoved() {
    if (!this._lifecycles.lastListenerRemoved)
      this._lifecycles.lastListenerRemoved = new Stream(`${this.name}LastListenerRemoved`);
    return this._lifecycles.lastListenerRemoved;
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
  get listenersCount() {
    return this._listeners.length;
  }
  get listeners() {
    return this._listeners.values();
  }

  [Symbol.dispose]() {
    this.terminate();
  }

  async *[Symbol.asyncIterator]() {
    let resolve: () => void;

    let head: { value: VALUE; next?: typeof head } | undefined;
    let tail: typeof head;
    const abort = this.listen((value) => {
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
          yield head.value;
          head = head.next;
          if (!head) tail = undefined;
        } else {
          await new Promise<void>((r) => (resolve = r));
        }
      }
    } finally {
      head = tail = undefined;
      abort();
    }
  }

  push(value: VALUE) {
    const listeners = this._listeners;
    const length = listeners.length;

    switch (length) {
      case 0:
        this._lifecycles.valueDropped?.push(value);
        return;
      case 1:
        listeners[0](value);
        return;
      case 2:
        listeners[0](value);
        listeners[1](value);
        return;

      default:
        for (let i = 0; i < length; i++) {
          listeners[i](value);
        }
    }
  }

  listen(listener: Stream.listener<VALUE>, options?: Stream.ListenOptions): Stream.Abort {
    if (this._lifecycles.isTerminated) throw new Error(`stream ${this.name} is terminated`);
    const { abortSignal } = options ?? {};

    abortSignal?.listenOnce(abort);

    const listeners = this._listeners;
    const lifecycles = this._lifecycles;

    listeners.push(listener);

    lifecycles.listenerAdded?.push(listener);

    if (this._listeners.length === 1) lifecycles.firstListenerAdded?.push(listener);

    return abort;

    function abort() {
      const index = listeners.indexOf(listener);
      if (index === -1) return;

      listeners.splice(index, 1);

      lifecycles.listenerRemoved?.push(listener);

      if (!listeners.length) lifecycles.lastListenerRemoved?.push(listener);
    }
  }
  listenOnce(listener: Stream.listener<VALUE>, options?: Stream.ListenOptions): Stream.Abort {
    const abort = this.listen((value) => {
      listener(value);
      abort();
    }, options);

    return abort;
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

    this._listeners.length = 0;

    this._lifecycles.terminated?.push();
    Object.values(this._lifecycles).forEach((lifecycle) => lifecycle instanceof Stream && lifecycle.terminate());
    this._lifecycles = { isTerminated: true };
  }
  clear() {
    this._listeners.length = 0;

    this._lifecycles.cleared?.push();
    Object.values(this._lifecycles).forEach((lifecycle) => lifecycle instanceof Stream && lifecycle.clear());
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type listener<VALUE> = (value: VALUE) => void;
  export type Abort = () => void;

  export type ListenOptions = { abortSignal?: AnyStream };
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

  // const abort = stream.listen(async (value) => {
  //   await new Promise((r) => setTimeout(r, Math.random() * 200));

  //   console.log("listener", value);
  // });

  (async () => {
    for await (const value of stream) {
      console.log("generator", value);
    }

    console.log("abort");
  })();

  stream.push(1);
  stream.push(2);
  stream.push(3);
}
function newStreamBench() {
  const MAX = 300_000_000;
  const now = performance.now();

  const stream = new Stream<number>();
  stream.listen((value) => {
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

// simpleTest();
// newStreamBench();
