import { Stream } from "./stream";

export class Source<VALUE, ERROR, NAME extends string> implements AsyncDisposable, Disposable {
  private _iterator: Iterator<VALUE | Source.Error<ERROR>> | AsyncIterator<VALUE | Source.Error<ERROR>>;
  private _idle = true;
  private _error?: Stream<ERROR, never, `${NAME}Error`>;

  constructor(
    public readonly name: NAME,
    sourceData: Source.SourceData<VALUE, ERROR>,
    private onNext: (value: VALUE) => void,
    private onDone: () => void,
  ) {
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

  private async asyncResult(resultPromise: Promise<IteratorResult<VALUE | Source.Error<ERROR>, any>>) {
    try {
      const result = await resultPromise;
      this._idle = true;
      if (result.done) {
        this.onDone();
      } else if (result.value instanceof Source.Error) {
        if (!this._error) throw result.value;
        this._error?.push(result.value.data);
      } else {
        this.onNext(result.value);
      }
    } catch (error: any) {
      this._idle = true;
      if (error instanceof Source.Error) {
        if (!this._error) throw error.data;
        this._error?.push(error.data);
      } else {
        if (!this._error) throw error;
        this._error?.push(error);
      }
    }
  }
  private syncResult(result: IteratorResult<VALUE | Source.Error<ERROR>, any>) {
    this._idle = true;
    if (result.done) {
      this.onDone();
    } else if (result.value instanceof Source.Error) {
      if (!this._error) throw result.value;
      this._error?.push(result.value.data);
    } else {
      this.onNext(result.value);
    }
  }
  requestNext() {
    this._idle = false;

    let result:
      | IteratorResult<VALUE | Source.Error<ERROR>, any>
      | Promise<IteratorResult<VALUE | Source.Error<ERROR>, any>>;
    try {
      result = this._iterator.next();
    } catch (error: any) {
      this._idle = true;
      if (error instanceof Source.Error) {
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
  async dispose() {
    await this._iterator.return?.();
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
  export class Error<const ERROR> {
    constructor(public readonly data: ERROR) {}
  }
  export type AnyError = Error<any>;
  export type SourceData<VALUE, ERROR> =
    | (() =>
        | AsyncGenerator<VALUE | Error<ERROR>>
        | Generator<VALUE | Error<ERROR>>
        | AsyncIterator<VALUE | Error<ERROR>>
        | Iterator<VALUE | Error<ERROR>>)
    | AsyncIterable<VALUE | Error<ERROR>>
    | Exclude<Iterable<VALUE | Error<ERROR>>, string>
    | AsyncIterator<VALUE | Error<ERROR>>
    | Iterator<VALUE | Error<ERROR>>;
}
