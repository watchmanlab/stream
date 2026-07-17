import type { Consumer } from "./consumer";
import { Closable } from "./types";

export class ScopeLinker {
  #consumers: Consumer.AnyConsumer[];
  constructor(
    private target: Closable,
    scope: ScopeLinker.Scope,
  ) {
    this.#consumers = [];

    if (scope.any && scope.any.length > 0) {
      this.#any(scope.any);
    } else if (scope.all && scope.all.length > 0) {
      this.#all(scope.all);
    }
  }

  #any(scopes: Closable[]): void {
    const set = new Set(scopes);

    scopes.forEach((scope) =>
      this.#consumers.push(
        scope.$aborted.listen((_) => {
          this.target.abort();
          set.clear();
        }),
        scope.$completed.listen(() => {
          this.target.complete();
          set.clear();
        }),
      ),
    );
  }
  #all(scopes: Closable[]): void {
    const set = new Set(scopes);
    let count = set.size;

    scopes.forEach((scope) => {
      this.#consumers.push(
        scope.$aborted.listen((_) => {
          this.target.abort();
          set.clear();
        }),
        scope.$completed.listen(() => {
          if (!--count) {
            this.target.complete();
            set.clear();
          }
        }),
      );
    });
  }
  abort() {
    for (const consumer of this.#consumers) consumer.abort();
    this.#consumers.length = 0;
  }
  complete() {
    for (const consumer of this.#consumers) consumer.complete();
    this.#consumers.length = 0;
  }
}

export namespace ScopeLinker {
  export type Scope = { any: [Closable, ...Closable[]]; all?: never } | { any?: never; all: [Closable, ...Closable[]] };
}
