import { Stream } from "./stream.ts";

//TODO: source need to be merged in the stream class
const NAME = "source";
export class Source<VALUE, ERROR, NAME extends string = Source.Name> implements AsyncDisposable, Disposable {
  readonly name: NAME;
  private _iterator: Iterator<VALUE> | AsyncIterator<VALUE>;
  private _idle = true;
  private _onNext?: (value: VALUE) => void;
  private _onDone?: () => void;
  private _error?: Stream<ERROR, never, `${NAME}Error`>;

  constructor(name = NAME as NAME, requirements: Source.Requirements<VALUE>) {
    this.name = name;
    this._onNext = requirements.onNext;
    this._onDone = requirements.onDone;

    if (typeof requirements.sourceData === "function") {
      this._iterator = requirements.sourceData();
    } else {
      this._iterator =
        (requirements.sourceData as any)[Symbol.asyncIterator]?.() ??
        (requirements.sourceData as any)[Symbol.iterator]();
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

    let result: IteratorResult<VALUE, any> | Promise<IteratorResult<VALUE, any>>;
    try {
      result = this._iterator.next();

      result = result instanceof Promise ? await result : result;

      this._idle = true;
      if (result.done) {
        this._onDone?.();
      } else {
        this._onNext?.(result.value);
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

    this._onDone?.();
    this._error = this._onDone = this._onNext = undefined;
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
  export type Requirements<VALUE> = {
    sourceData: Source.SourceData<VALUE>;
    onNext: (value: VALUE) => void;
    onDone: () => void;
  };
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
    | (() => AsyncGenerator<VALUE> | Generator<VALUE> | AsyncIterator<VALUE> | Iterator<VALUE>)
    | AsyncIterable<VALUE>
    | Exclude<Iterable<VALUE>, string>;
}
