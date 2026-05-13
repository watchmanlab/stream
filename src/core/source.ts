import { Stream } from "./stream.ts";

export class Source<VALUE, NAME extends string> implements AsyncDisposable, Disposable {
  private _iterator?: Iterator<Stream.Batch<VALUE>> | AsyncIterator<Stream.Batch<VALUE>> | Source.VoidIterator;
  private _requestingNext = false;
  private _error?: Stream<unknown, `${NAME}SourceError`>;
  private _done?: Stream<void, `${NAME}SourceDone`>;

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
  ready() {
    this._requestingNext = false;
  }
  pull() {
    if (!this._iterator || this._requestingNext) return;
    this._requestingNext = true;

    let result: IteratorResult<any> | void | Promise<IteratorResult<any> | void>;
    try {
      result = this._iterator!.next();

      if (!result) return;

      if (result instanceof Promise) {
        result
          .then((result) => {
            if (!result) return;
            if (result.done) return this.return();
            this.stream.batch(result.value);
            this.ready();
          })
          .catch((error) => this.throw(error));

        return;
      }

      if (result.done) {
        this.return();
      } else {
        this.stream.batch(result.value);
      }
      this.ready();
    } catch (error: any) {
      this._requestingNext = false;
      this.throw(error);
    }
  }
  throw(error: unknown) {
    this.ready();
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
  export type VoidIterator = {
    next: () => void | Promise<void>;
    return?: () => void | Promise<void>;
  };

  export type DataGenerator<VALUE> =
    | (() =>
        | VoidIterator
        | AsyncGenerator<Stream.Batch<VALUE>>
        | Generator<Stream.Batch<VALUE>>
        | AsyncIterator<Stream.Batch<VALUE>>
        | Iterator<Stream.Batch<VALUE>>)
    | AsyncIterable<Stream.Batch<VALUE>>
    | Iterable<Stream.Batch<VALUE>>;
}
