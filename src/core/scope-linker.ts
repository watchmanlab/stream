import { Stream } from "./stream";
import type { Consumer } from "./consumer";
import { Closable, CloseEvents, Evented } from "./types";

export class ScopeLinker {
  private _consumers: Consumer.AnyConsumer[];
  constructor(
    private target: Closable & Evented<CloseEvents>,
    public readonly scope: ScopeLinker.Scope,
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
    this._consumers.push(
      source.events.abort.listen((self, e) => this.target.abort(e.error)),
      source.events.complete.listen(() => this.target.complete()),
    );
  }
  private any(scopes: Stream.AnyStream[]): void {
    const set = new Set(scopes);

    scopes.forEach((scope) =>
      this._consumers!.push(
        scope.events.abort.listen((_, e) => {
          this.target.abort(e);
          set.clear();
        }),
        scope.events.complete.listen(() => {
          this.target.complete();
          set.clear();
        }),
      ),
    );
  }
  private all(scopes: Stream.AnyStream[]): void {
    const set = new Set(scopes);
    let count = set.size;

    scopes.forEach((scope) => {
      this._consumers!.push(
        scope.events.abort.listen((_, e) => {
          this.target.abort(e);
          set.clear();
        }),
        scope.events.complete.listen(() => {
          if (!--count) {
            this.target.complete();
            set.clear();
          }
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

export namespace ScopeLinker {
  export type Scope =
    | Stream.AnyStream
    | { any: [Stream.AnyStream, ...Stream.AnyStream[]]; all?: never }
    | { any?: never; all: [Stream.AnyStream, ...Stream.AnyStream[]] };
}
