import { Channel } from "./channel.ts";
import { Stream } from "./stream.ts";

export class Source<VALUE> implements Disposable {
  private _iterator?: Iterator<Stream.Batch<VALUE>> | AsyncIterator<Stream.Batch<VALUE>> | Channel<VALUE>;
  private _requestingNext = false;
  private _done?: Stream<void, `SourceDone`>;

  constructor(
    private stream: Stream.AnyStream,
    dataGenerator: Source.DataGenerator<VALUE>,
  ) {
    if (typeof dataGenerator === "function") {
      const result = dataGenerator();
      if (result instanceof Stream) {
        this._iterator = result.channels.get();
      } else {
        this._iterator = result;
      }
    } else if (dataGenerator instanceof Channel) {
      this._iterator = dataGenerator;
    } else if (dataGenerator instanceof Stream) {
      this._iterator = dataGenerator.channels.get();
    } else {
      this._iterator = (dataGenerator as any)[Symbol.asyncIterator]?.() ?? (dataGenerator as any)[Symbol.iterator]();
    }
  }
  [Symbol.dispose]() {
    this.return();
  }
  ready() {
    this._requestingNext = false;
  }
  next() {
    if (!this._iterator || this._requestingNext) return;
    this._requestingNext = true;

    let result: IteratorResult<any> | void | Promise<IteratorResult<any>>;
    try {
      result = this._iterator!.next();

      if (this._iterator instanceof Channel || !result) return;

      if (result instanceof Promise) {
        result
          .then((result) => {
            if (!result) return;
            if (result.done) return this.return();
            this.stream.batch(result.value);
            this.ready();
          })
          .catch((error) => {
            this.stream.batch([error]);
            this.ready();
          });

        return;
      }

      if (result.done) {
        this.return();
      } else {
        this.stream.batch(result.value);
        this.ready();
      }
    } catch (error) {
      this.stream.batch([error]);
      this.ready();
    }
  }

  return(): void {
    this._iterator?.return?.();

    this._done?.push();
    this._done?.dispose();

    this._iterator = this._done = undefined;
  }

  get done() {
    if (!this._done) {
      this._done = new Stream(`SourceDone`);
    }
    return this._done;
  }
}

export namespace Source {
  export interface VoidIterator {
    next: () => void;
    return?: () => void;
  }
  export type DataGenerator<VALUE> =
    | (() =>
        | AsyncGenerator<Stream.Batch<VALUE>>
        | Generator<Stream.Batch<VALUE>>
        | AsyncIterator<Stream.Batch<VALUE>>
        | Iterator<Stream.Batch<VALUE>>
        | Channel<VALUE>
        | Stream<VALUE, any>)
    | AsyncIterable<Stream.Batch<VALUE>>
    | Iterable<Stream.Batch<VALUE>>
    | Channel<VALUE>
    | Stream<VALUE, any>;

  export class Error<T, SOURCE extends Stream.AnyStream> extends globalThis.Error {
    constructor(
      public readonly data: T,
      public readonly source: SOURCE,
    ) {
      super(typeof data === "string" ? data : "");
    }
    get sourceName(): SOURCE["name"] {
      return this.source.name;
    }
    [Symbol.toStringTag] = `${this.sourceName} error`;
  }
}
