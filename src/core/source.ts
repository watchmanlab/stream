import { Channel } from "./channel";
import { type Stream } from "./stream";

export class Source<VALUE> implements Disposable {
  private _next?: () => void;
  private _return?: () => void;

  constructor(private options: Source.Options<VALUE>) {
    const result = typeof options.sourceData === "function" ? options.sourceData() : options.sourceData;
    if (result instanceof Channel) {
      this._next = () => result.next();
      this._return = () => result.return();
    } else if (Symbol.iterator in result) {
      const iterator = result[Symbol.iterator]();
      this._next = () => {
        const result = iterator.next();

        if (result.done) {
          this.return();
          return;
        }

        options.next(result.value);
      };
      this._return = () => iterator.return?.();
    } else if (Symbol.asyncIterator in result) {
      const iterator = result[Symbol.asyncIterator]();
      this._next = async () => {
        const result = await iterator.next();
        if (result.done) {
          this.return();
          return;
        }
        options.next(result.value);
      };
      this._return = () => iterator.return?.();
    } else {
      this._next = () => {
        const next = result.next();
        if (next instanceof Promise) {
          next.then((result) => {
            if (result.done) {
              this.return();
              return;
            }
            options.next(result.value);
          });
          this._next = () => {
            const next = result.next() as Promise<IteratorResult<VALUE>>;
            next.then((result) => {
              if (result.done) {
                this.return();
                return;
              }

              options.next(result.value);
            });
          };
        } else {
          if (next.done) {
            this.return();
            return;
          }
          options.next(next.value);
          this._next = () => {
            const next = result.next() as IteratorResult<VALUE>;
            if (next.done) {
              this.return();
              return;
            }
            options.next(next.value);
          };
        }
      };
      this._return = () => result.return?.();
    }
  }
  [Symbol.dispose]() {
    this.return();
  }

  next(): void {
    this._next?.();
  }
  return(): void {
    this._return?.();
    this.options.return?.();
    this._return = this._next = undefined;
  }
}

export namespace Source {
  export type SourceData<VALUE> =
    | Channel<Stream.Batch<VALUE>>
    | AsyncGenerator<VALUE>
    | Generator<VALUE>
    | AsyncIterator<VALUE>
    | Iterator<VALUE>
    | Iterable<VALUE>
    | AsyncIterable<VALUE>;
  export type SourceDataFunction<VALUE> = () => SourceData<VALUE>;

  export type Options<VALUE> = {
    sourceData: SourceData<VALUE> | SourceDataFunction<VALUE>;
    next: (value: VALUE) => void;
    return?: () => void;
  };
}
