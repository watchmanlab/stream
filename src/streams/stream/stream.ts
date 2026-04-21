const NAME = "stream";

export class Stream<VALUE = void, NAME extends string = Stream.Name> implements AsyncIterable<VALUE, void>, Disposable {
  protected listeners: Stream.Listener<VALUE>[] = [];
  private lifecycles: {
    listenerAdded?: Stream<Stream.Listener<VALUE>, `${NAME}ListenerAdded`>;
    firstListenerAdded?: Stream<Stream.Listener<VALUE>, `${NAME}FirstListenerAdded`>;
    listenerRemoved?: Stream<Stream.Listener<VALUE>, `${NAME}ListenerRemoved`>;
    lastListenerRemoved?: Stream<Stream.Listener<VALUE>, `${NAME}LastListenerRemoved`>;
    valueDropped?: Stream<VALUE, `${NAME}ValueDropped`>;
    terminated?: Stream<void, `${NAME}Terminated`>;
  } = {};

  constructor(public readonly name = NAME as NAME) {}
  get listenersCount() {
    return this.listeners.length;
  }

  get listenerAdded() {
    if (!this.lifecycles.listenerAdded) this.lifecycles.listenerAdded = new Stream(`${this.name}ListenerAdded`);
    return this.lifecycles.listenerAdded;
  }
  get firstListenerAdded() {
    if (!this.lifecycles.firstListenerAdded)
      this.lifecycles.firstListenerAdded = new Stream(`${this.name}FirstListenerAdded`);
    return this.lifecycles.firstListenerAdded;
  }
  get listenerRemoved() {
    if (!this.lifecycles.listenerRemoved) this.lifecycles.listenerRemoved = new Stream(`${this.name}ListenerRemoved`);
    return this.lifecycles.listenerRemoved;
  }
  get lastListenerRemoved() {
    if (!this.lifecycles.lastListenerRemoved)
      this.lifecycles.lastListenerRemoved = new Stream(`${this.name}LastListenerRemoved`);
    return this.lifecycles.lastListenerRemoved;
  }
  get valueDropped() {
    if (!this.lifecycles.valueDropped) this.lifecycles.valueDropped = new Stream(`${this.name}ValueDropped`);
    return this.lifecycles.valueDropped;
  }
  get terminated() {
    if (!this.lifecycles.terminated) this.lifecycles.terminated = new Stream(`${this.name}Terminated`);
    return this.lifecycles.terminated;
  }

  terminate() {
    this.lifecycles.listenerAdded?.terminate();
    delete this.lifecycles.listenerAdded;
    this.lifecycles.firstListenerAdded?.terminate();
    delete this.lifecycles.firstListenerAdded;
    this.lifecycles.listenerRemoved?.terminate();
    delete this.lifecycles.listenerRemoved;
    this.lifecycles.lastListenerRemoved?.terminate();
    delete this.lifecycles.lastListenerRemoved;
    this.lifecycles.valueDropped?.terminate();
    delete this.lifecycles.valueDropped;

    if (this.listeners.length) {
      this.listeners.length = 0;
    }
    this.lifecycles.terminated?.push();
    this.lifecycles.terminated?.terminate();
    delete this.lifecycles.terminated;
  }
  async *[Symbol.asyncIterator]() {
    const queue: VALUE[] = [];
    let resolve: Function = Function();

    const abort = this.listen((value) => {
      queue.push(value);
      resolve();
    });

    try {
      while (true) {
        if (queue.length) {
          yield queue.shift()!;
        } else {
          await new Promise<void>((r) => (resolve = r));
        }
      }
    } finally {
      abort();
      resolve();
      queue.length = 0;
    }
  }
  [Symbol.dispose]() {
    this.terminate();
  }

  push(value: VALUE) {
    const listeners = this.listeners;
    const length = this.listeners.length;
    if (!length) {
      this.lifecycles.valueDropped?.push(value);
      return;
    }
    for (let i = 0; i < length; i++) {
      listeners[i](value);
    }
  }

  listen(fn: Stream.Listener<VALUE>, abortSignal?: Stream.AnyStream): Stream.Abort {
    abortSignal?.listenOnce(abort);

    this.listeners.push(fn);

    this.lifecycles.listenerAdded?.push(fn);

    if (this.listeners.length === 1) this.lifecycles.firstListenerAdded?.push(fn);

    abort[Symbol.dispose] = abort;

    const self = this;

    return abort;

    function abort() {
      const index = self.listeners?.indexOf(fn) ?? -1;
      if (index === -1) return;

      self.listeners.splice(index, 1);

      self.lifecycles.listenerRemoved?.push(fn);

      if (!self.listeners.length) self.lifecycles.lastListenerRemoved?.push(fn);
    }
  }
  listenOnce(fn: Stream.Listener<VALUE>, abortSignal?: Stream.AnyStream): void {
    const abort = this.listen((value) => {
      fn(value);
      abort();
    }, abortSignal);
  }
  next(): Promise<VALUE> {
    return new Promise<VALUE>((resolve) => this.listenOnce(resolve));
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
}

export namespace Stream {
  export type Name = typeof NAME;
  export type Abort = { (): void } & Disposable;
  export type Listener<VALUE> = (value: VALUE) => any;
  export type ListenOptions<VALUE> = {
    startSignal?: Stream.AnyStream;
    stopSignal?: Stream.AnyStream;
    buffer?: Stream<VALUE, any>;
  };
  export type AsyncListener<VALUE> = (value: VALUE) => Promise<any>;
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
}
