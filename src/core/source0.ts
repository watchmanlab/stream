import { Consumable } from "./consumable0";

export interface Source<VALUE> extends Consumable<VALUE>, AsyncIterable<VALUE> {
  readonly pipe: <OUTPUT>(
    transform: ($input: Consumable<VALUE>) => OUTPUT,
  ) => OUTPUT extends Consumable<infer U> ? Source<U> : OUTPUT;
}

export namespace Source {
  export function from<VALUE>($consumable: Consumable<VALUE>): Source<VALUE> {
    return {
      consume: $consumable.consume,

      [Symbol.asyncIterator]() {
        return Consumable.toAsyncIterable(this)[Symbol.asyncIterator]();
      },

      pipe(transform) {
        return Consumable.pipe(this, transform);
      },
    };
  }
}
