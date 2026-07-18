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
        scope.$terminated.listen((_, reason) => {
          this.target.terminate(reason);
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
        scope.$terminated.listen((_, reason) => {
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
    for (const consumer of this.#consumers) consumer.terminate(reason);
    this.#consumers.length = 0;
  }
}

export namespace ScopeLinker {
  export type Scope = { any: [Closable, ...Closable[]]; all?: never } | { any?: never; all: [Closable, ...Closable[]] };
}
