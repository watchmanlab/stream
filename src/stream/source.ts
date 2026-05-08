import { Stream } from "./stream.ts";

export class Source<VALUE, ERROR, NAME extends string> implements AsyncDisposable, Disposable {
  readonly name: NAME;
  private _iterator:
    | Iterator<Source.SourceDataResult<VALUE, ERROR>>
    | AsyncIterator<Source.SourceDataResult<VALUE, ERROR>>;
  private _idle = true;
  private _error?: Stream<ERROR, never, `${NAME}Error`>;

  constructor(
    name: NAME,
    sourceData: Source.SourceData<VALUE, ERROR>,
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

  async requestNext() {
    this._idle = false;

    let result:
      | IteratorResult<Source.SourceDataResult<VALUE, ERROR>, any>
      | Promise<IteratorResult<Source.SourceDataResult<VALUE, ERROR>, any>>;
    try {
      result = this._iterator.next();

      result = result instanceof Promise ? await result : result;

      this._idle = true;
      if (result.done) {
        this.onDone();
      } else {
        const { values, errors } = result.value;

        if (errors.length) {
          if (!this._error) throw errors;
          this._error?.batch(errors);
        }

        if (values.length) {
          this.onNext(values);
        }
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
      this.requestNext();
    }
  }
  async dispose() {
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
  export type AnySource = Source<any, any, any>;
  export type AnySourceData = SourceData<any, any>;
  export type AnyError = Source.Error<any>;
  export type ExtractValue<T> = T extends Source<infer VALUE, any, any> | SourceData<infer VALUE, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnySource ? T["name"] : never;
  export type ExtractError<T> = T extends Source<any, infer ERROR, any> | SourceData<any, infer ERROR>
    ? ERROR
    : T extends AnyError
      ? T["data"]
      : never;
  export class Error<const ERROR> {
    constructor(public readonly data: ERROR) {}
  }
  export type SourceDataResult<VALUE, ERROR> = { values: Stream.Batch<VALUE>; errors: Stream.Batch<ERROR> };

  export type SourceData<VALUE, ERROR> =
    | (() =>
        | AsyncGenerator<SourceDataResult<VALUE, ERROR>>
        | Generator<SourceDataResult<VALUE, ERROR>>
        | AsyncIterator<SourceDataResult<VALUE, ERROR>>
        | Iterator<SourceDataResult<VALUE, ERROR>>)
    | AsyncIterable<SourceDataResult<VALUE, ERROR>>
    | Exclude<Iterable<SourceDataResult<VALUE, ERROR>>, string>;
  // export class SourceError<ERROR, SOURCE extends Stream.AnyStream> {
  //   constructor(
  //     public readonly error: ERROR,
  //     public readonly source: SOURCE,
  //   ) {}
  //   get sourceName(): SOURCE["name"] {
  //     return this.source.name;
  //   }
  // }
}
