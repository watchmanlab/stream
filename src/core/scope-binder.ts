import { Stream } from "./stream";
import type { Consumer } from "./consumer";

export class ScopeBinder {
  private _consumers: Consumer.AnyConsumer[];
  constructor(
    private target: Stream.AnyStream,
    public readonly scope: ScopeBinder.Scope,
  ) {
    this._consumers = [];

    if (scope instanceof Stream) {
      this.one(scope);
    } else if (scope.any) {
      this.any(scope.any);
    } else {
      this.all(scope.all);
    }
  }

  private one(source: Stream.AnyStream): void {
    const consumer = source.event.listen((self, e) => {
      switch (e.type) {
        case "abort":
          this.target.abort(e.error);
          break;
        case "complete":
          this.target.complete();
          break;
      }
      self.next();
    });
    this._consumers.push(consumer);
  }
  private any(scopes: Stream.AnyStream[]): void {
    const set = new Set(scopes);

    scopes.forEach((scope) =>
      this._consumers!.push(
        scope.event.listen((self, e) => {
          switch (e.type) {
            case "abort":
              this.target.abort(e.error);
              set.clear();
              break;
            case "complete":
              this.target.complete();
              set.clear();
              break;
          }
          self.next();
        }),
      ),
    );
  }
  private all(scopes: Stream.AnyStream[]): void {
    const set = new Set(scopes);
    let count = set.size;

    scopes.forEach((scope) => {
      this._consumers!.push(
        scope.event.listen((self, e) => {
          switch (e.type) {
            case "abort":
              this.target.abort(e.error);
              set.clear();
              break;
            case "complete":
              if (!--count) {
                this.target.complete();
                set.clear();
              }
              break;
          }
          self.next();
        }),
      );
    });
  }
  abort(error?: any): void {
    for (const consumer of this._consumers) {
      consumer.abort(error);
    }
    this._consumers.length = 0;
  }
  complete(): void {
    for (const consumer of this._consumers) {
      consumer.complete();
    }
    this._consumers.length = 0;
  }
}

export namespace ScopeBinder {
  export type Scope =
    | Stream.AnyStream
    | { any: [Stream.AnyStream, ...Stream.AnyStream[]]; all?: never }
    | { any?: never; all: [Stream.AnyStream, ...Stream.AnyStream[]] };
}
