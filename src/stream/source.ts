import { Stream } from "./stream.ts";

export class Source<VALUE, ERROR, NAME extends string> implements AsyncDisposable, Disposable {
  readonly name: NAME;
  private _iterator:
    | Iterator<Stream.Batch<VALUE | Source.Error<ERROR>>>
    | AsyncIterator<Stream.Batch<VALUE | Source.Error<ERROR>>>;
  private _idle = true;
  private _error?: Stream<ERROR, never, `${NAME}Error`>;

  constructor(
    name: NAME,
    sourceData: Source.SourceData<VALUE, ERROR>,
    private onNext: (value: VALUE[]) => void,
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
      | IteratorResult<(VALUE | Source.Error<ERROR>)[], any>
      | Promise<IteratorResult<(VALUE | Source.Error<ERROR>)[], any>>;
    try {
      result = this._iterator.next();

      result = result instanceof Promise ? await result : result;

      this._idle = true;
      if (result.done) {
        this.onDone();
      } else {
        let batch: VALUE[] = [];
        for (let i = 0; i < result.value.length; i++) {
          const value = result.value[i];
          if (value instanceof Source.Error) {
            if (!this._error) throw value.data;
            this._error?.push(value.data);
          } else {
            batch.push(value);
          }
        }

        this.onNext(batch);
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
  export type ExtractValue<T> =
    T extends Source<infer VALUE, any, any> ? VALUE : T extends SourceData<infer VALUE, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnySource ? T["name"] : never;
  export type ExtractError<T> =
    T extends Source<any, infer ERROR, any>
      ? ERROR
      : T extends SourceData<any, infer ERROR>
        ? ERROR
        : T extends AnyError
          ? T["data"]
          : never;
  export class Error<const ERROR> {
    constructor(public readonly data: ERROR) {}
  }

  export type SourceData<VALUE, ERROR> =
    | (() =>
        | AsyncGenerator<Stream.Batch<VALUE | Error<ERROR>>>
        | Generator<Stream.Batch<VALUE | Error<ERROR>>>
        | AsyncIterator<Stream.Batch<VALUE | Error<ERROR>>>
        | Iterator<Stream.Batch<VALUE | Error<ERROR>>>)
    | AsyncIterable<Stream.Batch<VALUE | Error<ERROR>>>
    | Exclude<Iterable<Stream.Batch<VALUE | Error<ERROR>>>, string>;
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
