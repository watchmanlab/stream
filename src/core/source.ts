import { Stream } from "./stream.ts";

const NAME = "source";
export class Source<VALUE, ERROR, NAME extends string = Source.Name> implements AsyncDisposable, Disposable {
  readonly name: NAME;
  private _iterator: Iterator<Stream.Batch<VALUE>> | AsyncIterator<Stream.Batch<VALUE>>;
  private _idle = true;
  private _error?: Stream<ERROR, never, `${NAME}Error`>;

  constructor(
    name = NAME as NAME,
    sourceData: Source.SourceData<VALUE>,
    private onNext: (value: Stream.Batch<VALUE>) => void,
    private onDone: () => void,
  ) {
    this.name = name;

    if (typeof sourceData === "function") {
      this._iterator = sourceData();
    } else {
      this._iterator = (sourceData as any)[Symbol.asyncIterator]?.() ?? (sourceData as any)[Symbol.iterator]();
    }
  }
  async [Symbol.asyncDispose]() {
    await this.dispose();
  }
  [Symbol.dispose]() {
    this.dispose();
  }
  throw(error: ERROR) {
    if (!this._error?.consumersCount)
      Promise.reject(
        `Unhandled error in "${this.name}": ${error}

Consume ${this.name}.source.error to handle this.
`,
      );
    this._error?.push(error);
  }
  async requestNext() {
    this._idle = false;

    let result: IteratorResult<Stream.Batch<VALUE>, any> | Promise<IteratorResult<Stream.Batch<VALUE>, any>>;
    try {
      result = this._iterator.next();

      result = result instanceof Promise ? await result : result;

      this._idle = true;
      if (result.done) {
        this.onDone();
      } else {
        this.onNext(result.value);
      }
    } catch (error: any) {
      this._idle = true;
      if (!this._error) throw error;
      this._error?.push(error);
      this.requestNext();
    }
  }
  async dispose() {
    this._idle = false;
    await Promise.all([this._iterator.return?.(), this._error?.dispose()]);
    this._error = undefined;
    this.onDone();
  }
  get idle() {
    return this._idle;
  }
  get error() {
    if (!this._error) this._error = new Stream(`${this.name}Error`);
    return this._error;
  }
}

export namespace Source {
  export type Name = typeof NAME;
  export type AnySource = Source<any, any, any>;
  export type AnySourceData = SourceData<any>;
  export type AnyError = Source.Error<any>;
  export type ExtractValue<T> = T extends Source<infer VALUE, any, any> | SourceData<infer VALUE> ? VALUE : never;
  export type ExtractName<T> = T extends AnySource ? T["name"] : never;
  export type ExtractError<T> =
    T extends Source<any, infer ERROR, any> ? ERROR : T extends AnyError ? T["data"] : never;
  export class Error<const ERROR> {
    constructor(public readonly data: ERROR) {}
  }

  export type SourceData<VALUE> =
    | (() =>
        | AsyncGenerator<Stream.Batch<VALUE>>
        | Generator<Stream.Batch<VALUE>>
        | AsyncIterator<Stream.Batch<VALUE>>
        | Iterator<Stream.Batch<VALUE>>)
    | AsyncIterable<Stream.Batch<VALUE>>
    | Exclude<Iterable<Stream.Batch<VALUE>>, string>;
}
