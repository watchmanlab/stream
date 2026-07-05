import type { Consumer } from "./consumer";
import { Closable, CloseEvents, Evented } from "./types";

export class ScopeLinker {
  private _consumers: Consumer.AnyConsumer[];
  constructor(
    private target: Closable,
    scope: ScopeLinker.Scope,
  ) {
    this._consumers = [];

    if (scope.any && scope.any.length > 0) {
      this.any(scope.any);
    } else if (scope.all && scope.all.length > 0) {
      this.all(scope.all);
    }
  }

  private any(scopes: Evented<CloseEvents>[]): void {
    const set = new Set(scopes);

    scopes.forEach((scope) =>
      this._consumers.push(
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
  private all(scopes: Evented<CloseEvents>[]): void {
    const set = new Set(scopes);
    let count = set.size;

    scopes.forEach((scope) => {
      this._consumers.push(
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
  abort(error?: any) {
    for (const consumer of this._consumers) consumer.abort(error);
    this._consumers.length = 0;
  }
  complete() {
    for (const consumer of this._consumers) consumer.complete();
    this._consumers.length = 0;
  }
}

export namespace ScopeLinker {
  export type Scope =
    | { any: [Evented<CloseEvents>, ...Evented<CloseEvents>[]]; all?: never }
    | { any?: never; all: [Evented<CloseEvents>, ...Evented<CloseEvents>[]] };
}
