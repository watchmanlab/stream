const NAME = "stream";

export class Stream<VALUE = void, NAME extends string = Stream.Name> implements AsyncIterable<VALUE, void>, Disposable {
  protected listeners: Stream.Listener<VALUE>[] = [];
  private lifecycles: {
    beforeListenerAdded?: Stream<Stream.Listener<VALUE>, `${NAME}BeforeListenerAdded`>;
    afterListenerAdded?: Stream<Stream.Listener<VALUE>, `${NAME}AfterListenerAdded`>;
    afterFirstListenerAdded?: Stream<Stream.Listener<VALUE>, `${NAME}AfterFirstListenerAdded`>;
    beforeListenerRemoved?: Stream<Stream.Listener<VALUE>, `${NAME}BeforeListenerRemoved`>;
    afterListenerRemoved?: Stream<Stream.Listener<VALUE>, `${NAME}AfterListenerRemoved`>;
    afterLastListenerRemoved?: Stream<Stream.Listener<VALUE>, `${NAME}AfterLastListenerRemoved`>;
    beforePush?: Stream<VALUE, `${NAME}BeforePush`>;
    afterPush?: Stream<{ value: VALUE; results: any[] }, `${NAME}AfterPush`>;
    valueDropped?: Stream<VALUE, `${NAME}ValueDropped`>;
    beforeTerminate?: Stream<VALUE[], `${NAME}BeforeTerminate`>;
    afterTerminate?: Stream<VALUE[], `${NAME}AfterTerminate`>;
  } = {};

  constructor(public readonly name = NAME as NAME) {}
  get listenersCount() {
    return this.listeners.length;
  }
  get beforeListenerAdded() {
    if (!this.lifecycles.beforeListenerAdded)
      this.lifecycles.beforeListenerAdded = new Stream(`${this.name}BeforeListenerAdded`);
    return this.lifecycles.beforeListenerAdded;
  }
  get afterListenerAdded() {
    if (!this.lifecycles.afterListenerAdded)
      this.lifecycles.afterListenerAdded = new Stream(`${this.name}AfterListenerAdded`);
    return this.lifecycles.afterListenerAdded;
  }
  get afterFirstListenerAdded() {
    if (!this.lifecycles.afterFirstListenerAdded)
      this.lifecycles.afterFirstListenerAdded = new Stream(`${this.name}AfterFirstListenerAdded`);
    return this.lifecycles.afterFirstListenerAdded;
  }
  get beforeListenerRemoved() {
    if (!this.lifecycles.beforeListenerRemoved)
      this.lifecycles.beforeListenerRemoved = new Stream(`${this.name}BeforeListenerRemoved`);
    return this.lifecycles.beforeListenerRemoved;
  }
  get afterListenerRemoved() {
    if (!this.lifecycles.afterListenerRemoved)
      this.lifecycles.afterListenerRemoved = new Stream(`${this.name}AfterListenerRemoved`);
    return this.lifecycles.afterListenerRemoved;
  }
  get afterLastListenerRemoved() {
    if (!this.lifecycles.afterLastListenerRemoved)
      this.lifecycles.afterLastListenerRemoved = new Stream(`${this.name}AfterLastListenerRemoved`);
    return this.lifecycles.afterLastListenerRemoved;
  }
  get beforePush() {
    if (!this.lifecycles.beforePush) this.lifecycles.beforePush = new Stream(`${this.name}BeforePush`);
    return this.lifecycles.beforePush;
  }
  get afterPush() {
    if (!this.lifecycles.afterPush) this.lifecycles.afterPush = new Stream(`${this.name}AfterPush`);
    return this.lifecycles.afterPush;
  }
  get valueDropped() {
    if (!this.lifecycles.valueDropped) this.lifecycles.valueDropped = new Stream(`${this.name}ValueDropped`);
    return this.lifecycles.valueDropped;
  }
  get beforeTerminate() {
    if (!this.lifecycles.beforeTerminate) this.lifecycles.beforeTerminate = new Stream(`${this.name}BeforeTerminate`);
    return this.lifecycles.beforeTerminate;
  }
  get afterTerminate() {
    if (!this.lifecycles.afterTerminate) this.lifecycles.afterTerminate = new Stream(`${this.name}AfterTerminate`);
    return this.lifecycles.afterTerminate;
  }

  terminate() {
    this.lifecycles.beforeListenerAdded?.terminate();
    delete this.lifecycles.beforeListenerAdded;
    this.lifecycles.afterListenerAdded?.terminate();
    delete this.lifecycles.afterListenerAdded;
    this.lifecycles.afterFirstListenerAdded?.terminate();
    delete this.lifecycles.afterFirstListenerAdded;
    this.lifecycles.beforeListenerRemoved?.terminate();
    delete this.lifecycles.beforeListenerRemoved;
    this.lifecycles.afterListenerRemoved?.terminate();
    delete this.lifecycles.afterListenerRemoved;
    this.lifecycles.afterLastListenerRemoved?.terminate();
    delete this.lifecycles.afterLastListenerRemoved;
    this.lifecycles.beforePush?.terminate();
    delete this.lifecycles.beforePush;
    this.lifecycles.afterPush?.terminate();
    delete this.lifecycles.afterPush;
    this.lifecycles.valueDropped?.terminate();
    delete this.lifecycles.valueDropped;

    this.lifecycles.beforeTerminate?.push();
    this.lifecycles.beforeTerminate?.terminate();
    delete this.lifecycles.beforeTerminate;
    this.listeners.length = 0;
    this.lifecycles.afterTerminate?.push();
    this.lifecycles.afterTerminate?.terminate();
    delete this.lifecycles.afterTerminate;
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
  push(...values: VALUE[]) {
    const listeners = this.listeners;
    const listenersLenght = this.listeners.length;
    const valuesLength = values.length;

    for (let i = 0; i < valuesLength; i++) {
      const value = values[i];
      this.lifecycles.beforePush?.push(value);
      if (!listenersLenght) {
        this.lifecycles.valueDropped?.push(value);
      } else if (this.lifecycles.afterPush) {
        const valueResults = { value, results: [] as any[] };
        for (let j = 0; j < listenersLenght; j++) {
          valueResults.results.push(listeners[j](value));
        }
        this.lifecycles.afterPush.push(valueResults);
      } else {
        for (let j = 0; j < listenersLenght; j++) {
          listeners[j](value);
        }
      }
    }
  }

  listen(fn: Stream.Listener<VALUE>, signal?: Stream.AnyStream): Stream.Abort {
    signal?.listenOnce(abort);

    this.lifecycles.beforeListenerAdded?.push(fn);

    this.listeners.push(fn);

    this.lifecycles.afterListenerAdded?.push(fn);

    if (this.listeners.length !== 1) return abort;

    this.lifecycles.afterFirstListenerAdded?.push(fn);

    const self = this;

    return abort;
    function abort() {
      if (!self.listeners.length) return;

      const index = self.listeners?.indexOf(fn) ?? -1;
      if (index === -1) return;

      self.listeners.splice(index, 1);

      self.lifecycles.afterListenerRemoved?.push(fn);

      if (!self.listeners.length) {
        self.listeners.length = 0;
        self.lifecycles.afterLastListenerRemoved?.push(fn);
      }
    }
  }
  listenOnce(fn: Stream.Listener<VALUE>, signal?: Stream.AnyStream): void {
    const abort = this.listen((value) => {
      fn(value);
      abort();
    }, signal);
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

export class Consumer<VALUE> {
  constructor(private fn: Stream.Listener<VALUE>) {
    //
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type Abort = () => void;
  export type Listener<VALUE> = (value: VALUE) => any;
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
  export class TransformError<VALUE, ERROR, SOURCE extends AnyStream> {
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
