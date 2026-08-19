import { Consumer } from "./consumer0";

export interface Consumable<VALUE> {
  readonly consume: (handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>) => Consumer<VALUE>;
}

export namespace Consumable {
  export function pipe<$INPUT extends Consumable<any>, OUTPUT>(
    $input: $INPUT,
    transform: ($input: $INPUT) => OUTPUT,
  ): OUTPUT {
    return transform($input);
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
}
