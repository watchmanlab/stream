import { Channel } from "./channel.ts";
import { Stream } from "./stream.ts";

export class Source<VALUE> implements Disposable {
  private _ready = true;

  private _next?: () => void;
  private _return?: () => void;

  constructor(stream: Stream.AnyStream, sourceData: Source.SourceData<VALUE> | Source.SourceDataFunction<VALUE>) {
    const result = typeof sourceData === "function" ? sourceData() : sourceData;
    if (result instanceof Channel) {
      this._next = () => result.next();
      this._return = () => result.return();
    } else if (result instanceof Stream) {
      const channel = result.channels.get({ next: (batch) => stream.batch(batch), return: () => stream.dispose() });
      this._next = () => channel.next();
      this._return = () => channel.return();
    } else if (Symbol.iterator in result) {
      const iterator = result[Symbol.iterator]();
      this._next = () => {
        const result = iterator.next();
        if (result.done) {
          this.return();
          return;
        }
        stream.batch([result.value]);
        this.ready();
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
        stream.batch([result.value]);
        this.ready();
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
            stream.batch([result.value]);
            this.ready();
          });
          this._next = () => {
            const next = result.next() as Promise<IteratorResult<VALUE>>;
            next.then((result) => {
              if (result.done) {
                this.return();
                return;
              }
              stream.batch([result.value]);
              this.ready();
            });
          };
        } else {
          if (!Array.isArray(next?.value) || next.done) {
            this.return();
            return;
          }
          stream.batch(next.value);
          this.ready();
          this._next = () => {
            const next = result.next() as IteratorResult<VALUE>;
            if (next.done) {
              this.return();
              return;
            }
            stream.batch([next.value]);
            this.ready();
          };
        }
      };
      this._return = () => result.return?.();
    }
  }
  [Symbol.dispose]() {
    this.return();
  }
  ready(): void {
    this._ready = true;
  }
  next(): void {
    if (!this._next || !this._ready) return;
    this._ready = false;
    this._next?.();
  }
  return(): void {
    this._return?.();
    this._return = this._next = undefined;
  }
}

export namespace Source {
  export type VoidIterator = {
    next: () => void;
    return?: () => void;
  };
  export type SourceData<VALUE> =
    | VoidIterator
    | AsyncGenerator<VALUE>
    | Generator<VALUE>
    | AsyncIterator<VALUE>
    | Iterator<VALUE>
    | Channel<VALUE>
    | Stream<VALUE, any>
    | Iterable<VALUE>
    | AsyncIterable<VALUE>;
  export type SourceDataFunction<VALUE> = () => SourceData<VALUE>;
}
