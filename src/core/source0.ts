// 1. THE PRISTINE, ANONYMOUS PRIMITIVE

import { Consumable } from "./consumable0";

// 2. THE PREMIUM COMPOSITION & ITERATION PRIMITIVE
export interface Source<VALUE> extends Consumable<VALUE>, AsyncIterable<VALUE> {
  readonly pipe: <OUTPUT>(
    transform: ($input: Consumable<VALUE>) => OUTPUT,
  ) => OUTPUT extends Consumable<infer U> ? Source<U> : OUTPUT;
}

export namespace Source {
  export function from<VALUE>(consumable: Consumable<VALUE>): Source<VALUE> {
    return {
      consume: consumable.consume,

      // 🏎️ Delegate directly to the pre-allocated global iterator implementation
      [Symbol.asyncIterator]() {
        return Consumable.toAsyncIterable(this)[Symbol.asyncIterator]();
      },

      pipe(transform) {
        const nextResult = transform(this);

        // Keep wrapping as long as the pipeline stays inside the Consumable domain
        if (nextResult && typeof (nextResult as any).consume === "function") {
          return from(nextResult as any) as any;
        }

        return nextResult as any;
      },
    };
  }
}
