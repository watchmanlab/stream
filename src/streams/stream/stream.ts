const NAME = "stream";

export class Stream<VALUE = void, NAME extends string = Stream.Name> implements AsyncIterable<VALUE, void>, Disposable {
  protected _listeners: Stream.Listener<VALUE>[] = [];
  private _beforeListenerAdded?: Stream<Stream.Listener<VALUE>, `${NAME}BeforeListenerAdded`>;
  private _afterListenerAdded?: Stream<Stream.Listener<VALUE>, `${NAME}AfterListenerAdded`>;
  private _afterFirstListenerAdded?: Stream<Stream.Listener<VALUE>, `${NAME}AfterFirstListenerAdded`>;
  private _beforeListenerRemoved?: Stream<Stream.Listener<VALUE>, `${NAME}BeforeListenerRemoved`>;
  private _afterListenerRemoved?: Stream<Stream.Listener<VALUE>, `${NAME}AfterListenerRemoved`>;
  private _afterLastListenerRemoved?: Stream<Stream.Listener<VALUE>, `${NAME}AfterLastListenerRemoved`>;
  private _beforePush?: Stream<VALUE[], `${NAME}BeforePush`>;
  private _afterPush?: Stream<VALUE[], `${NAME}AfterPush`>;
  private _afterValuesDropped?: Stream<VALUE[], `${NAME}AfterValuesDropped`>;
  private _beforeTerminate?: Stream<VALUE[], `${NAME}BeforeTerminate`>;
  private _afterTerminate?: Stream<VALUE[], `${NAME}AfterTerminate`>;

  constructor(public readonly name = NAME as NAME) {}
  get listenersCount() {
    return this._listeners.length;
  }
  get beforeListenerAdded() {
    if (!this._beforeListenerAdded) this._beforeListenerAdded = new Stream(`${this.name}BeforeListenerAdded`);
    return this._beforeListenerAdded;
  }
  get afterListenerAdded() {
    if (!this._afterListenerAdded) this._afterListenerAdded = new Stream(`${this.name}AfterListenerAdded`);
    return this._afterListenerAdded;
  }
  get afterFirstListenerAdded() {
    if (!this._afterFirstListenerAdded)
      this._afterFirstListenerAdded = new Stream(`${this.name}AfterFirstListenerAdded`);
    return this._afterFirstListenerAdded;
  }
  get beforeListenerRemoved() {
    if (!this._beforeListenerRemoved) this._beforeListenerRemoved = new Stream(`${this.name}BeforeListenerRemoved`);
    return this._beforeListenerRemoved;
  }
  get afterListenerRemoved() {
    if (!this._afterListenerRemoved) this._afterListenerRemoved = new Stream(`${this.name}AfterListenerRemoved`);
    return this._afterListenerRemoved;
  }
  get afterLastListenerRemoved() {
    if (!this._afterLastListenerRemoved)
      this._afterLastListenerRemoved = new Stream(`${this.name}AfterLastListenerRemoved`);
    return this._afterLastListenerRemoved;
  }
  get beforePush() {
    if (!this._beforePush) this._beforePush = new Stream(`${this.name}BeforePush`);
    return this._beforePush;
  }
  get afterPush() {
    if (!this._afterPush) this._afterPush = new Stream(`${this.name}AfterPush`);
    return this._afterPush;
  }
  get afterValuesDropped() {
    if (!this._afterValuesDropped) this._afterValuesDropped = new Stream(`${this.name}AfterValuesDropped`);
    return this._afterValuesDropped;
  }
  get beforeTerminate() {
    if (!this._beforeTerminate) this._beforeTerminate = new Stream(`${this.name}BeforeTerminate`);
    return this._beforeTerminate;
  }
  get afterTerminate() {
    if (!this._afterTerminate) this._afterTerminate = new Stream(`${this.name}AfterTerminate`);
    return this._afterTerminate;
  }
  terminate() {
    this._beforeListenerAdded?.terminate();
    this._beforeListenerAdded = undefined;
    this._afterListenerAdded?.terminate();
    this._afterListenerAdded = undefined;
    this._afterFirstListenerAdded?.terminate();
    this._afterFirstListenerAdded = undefined;
    this._beforeListenerRemoved?.terminate();
    this._beforeListenerRemoved = undefined;
    this._afterListenerRemoved?.terminate();
    this._afterListenerRemoved = undefined;
    this._afterLastListenerRemoved?.terminate();
    this._afterLastListenerRemoved = undefined;
    this._beforePush?.terminate();
    this._beforePush = undefined;
    this._afterPush?.terminate();
    this._afterPush = undefined;
    this._afterValuesDropped?.terminate();
    this._afterValuesDropped = undefined;

    this._beforeTerminate?.push();
    this._beforeTerminate?.terminate();
    this._beforeTerminate = undefined;
    this._listeners.length = 0;
    this._afterTerminate?.push();
    this._afterTerminate?.terminate();
    this._afterTerminate = undefined;
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
    const listeners = this._listeners;
    const listenersLenght = this._listeners.length;
    const valuesLength = values.length;
    const results: any[] = [];

    this._beforePush?.push(values);

    if (!listenersLenght) {
      this._afterValuesDropped?.push(values);
      return results;
    }

    for (let i = 0; i < valuesLength; i++) {
      const value = values[i];

      for (let j = 0; j < listenersLenght; j++) {
        results.push(listeners[j](value));
      }
    }

    this._afterPush?.push(values);

    return results;
  }

  listen(fn: Stream.Listener<VALUE>, signal?: Stream.AnyStream): Stream.Abort {
    signal?.listenOnce(abort);

    this._beforeListenerAdded?.push(fn);

    this._listeners.push(fn);

    this._afterListenerAdded?.push(fn);

    if (this._listeners.length !== 1) return abort;

    this._afterFirstListenerAdded?.push(fn);

    const self = this;

    return abort;
    function abort() {
      if (!self._listeners.length) return;

      const index = self._listeners?.indexOf(fn) ?? -1;
      if (index === -1) return;

      self._listeners.splice(index, 1);

      self._afterListenerRemoved?.push(fn);

      if (!self._listeners.length) {
        self._listeners.length = 0;
        self._afterLastListenerRemoved?.push(fn);
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

export namespace Stream {
  export type Name = typeof NAME;
  export type Abort = () => void;
  export type Listener<VALUE> = (value: VALUE) => any;
  export type AnyStream = Stream<any, any>;
  export type AnyTransformer = Transformer<AnyStream, AnyStream>;
  export type ExtractValue<T extends AnyStream> = T extends Stream<infer VALUE, any> ? VALUE : never;
  export type ExtractName<T extends AnyStream> = T extends Stream<any, infer NAME> ? NAME : never;
  export type ExtractError<T extends Err<any>> = T extends Err<infer ERROR> ? ERROR : never;

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

  export class SourceErr<VALUE, ERROR, SOURCE extends AnyStream> {
    constructor(
      public readonly value: VALUE,
      public readonly error: ERROR,
      public readonly source: SOURCE,
    ) {}
    get sourceName(): SOURCE["name"] {
      return this.source.name;
    }
  }
  export class Err<ERROR> {
    sourceName: string = "";
    constructor(public readonly value: ERROR) {}
  }
  export function err<ERROR>(value: ERROR): Err<ERROR> {
    return new Err(value);
  }
  export function isErr<ERROR>(object: unknown): object is Err<ERROR> {
    return object instanceof Err;
  }
}
