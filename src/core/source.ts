import { Stream } from "./stream.ts";

export class Source<VALUE, ERROR, NAME extends string> implements AsyncDisposable, Disposable {
  private _iterator?: Iterator<Stream.Batch<VALUE>> | AsyncIterator<Stream.Batch<VALUE>>;
  private _requestingNext = false;
  private _error?: Stream<ERROR, never, `${NAME}SourceError`>;
  private _done?: Stream<void, never, `${NAME}SourceDone`>;

  constructor(
    private stream: Stream.AnyStream,
    dataGenerator: Source.DataGenerator<VALUE>,
  ) {
    if (typeof dataGenerator === "function") {
      this._iterator = dataGenerator();
    } else {
      this._iterator = (dataGenerator as any)[Symbol.asyncIterator]?.() ?? (dataGenerator as any)[Symbol.iterator]();
    }
  }
  async [Symbol.asyncDispose]() {
    await this.return();
  }
  [Symbol.dispose]() {
    this.return();
  }
  next() {
    if (!this._iterator || this._requestingNext) return;

    this._requestingNext = true;

    let result: IteratorResult<Stream.Batch<VALUE>> | Promise<IteratorResult<Stream.Batch<VALUE>>>;
    (async () => {
      try {
        result = this._iterator!.next();

        result = result instanceof Promise ? await result : result;

        this._requestingNext = false;
        if (result.done) {
          await this.return();
        } else {
          this.stream.batch(result.value);
        }
      } catch (error: any) {
        this._requestingNext = false;
        this.throw(error);
      }
    })();
  }
  throw(error: ERROR) {
    if (!this._error?.channels.count)
      Promise.reject(
        `Unhandled error in "${this.stream.name}": ${error}\nConsume ${this.stream.name}.error to handle this.`,
      );
    this._error?.push(error);
  }
  async return() {
    await Promise.all([this._iterator?.return?.(), this._error?.dispose()]);
    this._done?.push();
    await this._done?.dispose();
    this._iterator = this._error = this._done = undefined;
  }

  get error() {
    if (!this._error) this._error = new Stream(`${this.stream.name}SourceError`);
    return this._error;
  }
  get done() {
    if (!this._done) {
      this._done = new Stream(`${this.stream.name}SourceDone`);
    }
    return this._done;
  }
}

export namespace Source {
  export type DataGenerator<VALUE> =
    | (() =>
        | AsyncGenerator<Stream.Batch<VALUE>>
        | Generator<Stream.Batch<VALUE>>
        | AsyncIterator<Stream.Batch<VALUE>>
        | Iterator<Stream.Batch<VALUE>>)
    | AsyncIterable<Stream.Batch<VALUE>>
    | Iterable<Stream.Batch<VALUE>>;

  export class Error<const ERROR> {
    constructor(public readonly data: ERROR) {}
  }
}
