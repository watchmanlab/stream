import type { Closable, AnyConsumer } from "./types";

export class ScopeBinder {
  private _consumers: AnyConsumer[];
  constructor(
    private target: Closable,
    scope: ScopeBinder.Scope,
  ) {
    this._consumers = [];

    if (scope.any && scope.any.length > 0) {
      this.any(scope.any);
    } else if (scope.all && scope.all.length > 0) {
      this.all(scope.all);
    }
  }

  private any(scopes: Closable[]): void {
    const set = new Set(scopes);

    scopes.forEach((scope) =>
      this._consumers.push(
        scope.$terminate.listen((_, reason) => {
          this.target.terminate(reason);
          set.clear();
        }),
      ),
    );
  }
  private all(scopes: Closable[]): void {
    const set = new Set(scopes);
    let count = set.size;

    scopes.forEach((scope) => {
      this._consumers.push(
        scope.$terminate.listen((_, reason) => {
          switch (reason) {
            case "abort":
              this.target.terminate("abort");
              break;
            case "complete":
              if (!--count) {
                this.target.terminate("complete");
                break;
              }
          }
          set.clear();
        }),
      );
    });
  }
  terminate(reason: "abort" | "complete") {
    for (const consumer of this._consumers) consumer.terminate(reason);
    this._consumers.length = 0;
  }
}

export namespace ScopeBinder {
  export type Scope = { any: [Closable, ...Closable[]]; all?: never } | { any?: never; all: [Closable, ...Closable[]] };
}
