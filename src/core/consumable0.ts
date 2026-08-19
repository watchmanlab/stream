import { Consumer } from "./consumer0";
import { Source } from "./source0";

export interface Consumable<VALUE> {
  readonly consume: (handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>) => Consumer<VALUE>;
}

export namespace Consumable {
  export type AnyConsumable = Consumable<any>;
  export function isConsumable<T>(object: unknown): object is Consumable<T> {
    return object !== null && typeof object === "object" && "consume" in object && typeof object.consume === "function";
  }
  export function toAsyncIterable<VALUE>($consumable: Consumable<VALUE>): AsyncIterable<VALUE> {
    return {
      async *[Symbol.asyncIterator]() {
        let resolve: ((value: VALUE) => void) | null = null;

        const consumer = $consumable.consume((_, value) => {
          if (resolve) {
            resolve(value);
            resolve = null;
          }
        });

        try {
          while (consumer.status === "active" || consumer.status === "drain") {
            yield new Promise<VALUE>((r) => {
              resolve = r;
              Consumer.next(consumer);
            });
          }
        } catch (e) {
          Consumer.terminate(consumer, "abort");
          throw e;
        } finally {
          Consumer.terminate(consumer, "complete");
        }
      },
    };
  }
  export function pipe<$INPUT extends AnyConsumable, OUTPUT>(
    $input: $INPUT,
    transform: ($input: $INPUT) => OUTPUT,
  ): OUTPUT extends Consumable<infer VALUE> ? OUTPUT & Source<VALUE> : OUTPUT {
    const output = transform($input);

    if (Consumable.isConsumable(output)) {
      return Source.from(output) as any;
    }

    return output as any;
  }
}
