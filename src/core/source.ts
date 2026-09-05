import { Consumable } from "./consumable";
import { Consumer } from "./consumer";

/**
 * Abstract base class for all streams, source adapters and transformers.
 * it's not a mandatory but very useful primitive to extends from ,
 * the library care only about {@link Consumable} contract,
 *
 * Provides:
 * - {@link pipe} for composing transformers in a readable chain.
 * - `Symbol.asyncIterator` for consuming values with `for await...of`.
 * - {@link Source.from} static factory to wrap any {@link Consumable}.
 *
 * @template VALUE The type of values emitted by this source.
 *
 * @example
 * const source = fromIterable([1, 2, 3]);
 * source.pipe(map(v => v * 2)).pipe(listen(console.log));
 *
 * @example
 * for await (const value of fromIterable([1, 2, 3])) {
 *   console.log(value);
 * }
 */
export abstract class Source<VALUE> implements Consumable<VALUE>, AsyncIterable<VALUE> {
  async *[Symbol.asyncIterator]() {
    let resolve: (value: VALUE) => void;

    const consumer = this.consume((_, value) => resolve(value));

    try {
      while (consumer.status === "active" || consumer.status === "drain") {
        yield new Promise<VALUE>((r) => {
          resolve = r;
          consumer.next();
        });
      }
    } catch (e) {
      consumer.terminate("abort");
    } finally {
      consumer.terminate("complete");
    }
  }
  abstract consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
  /**
   * Composes this source with a transformer function.
   * The transformer receives this source and returns a new output (usually another `Source`).
   *
   * @param transform A function that takes this source and returns a transformed output.
   * @returns The result of applying the transformer.
   */
  pipe<OUTPUT>(transform: ($input: this) => OUTPUT): OUTPUT {
    return transform(this);
  }
  /**
   * Wraps any {@link Consumable} in a `Source` proxy, giving it access to `pipe`
   * and async iteration without changing its consumption behaviour.
   * and it's used for wrapping a complex sources like {@link Stream} to encapsulate behaviors and get a readonly source.
   *
   * @param consumable Any consumable to wrap and/or hide behaviors .
   */
  static from<VALUE>($consumable: Consumable<VALUE>): Source<VALUE> {
    return new SourceProxy($consumable);
  }
}

export namespace Source {
  export type AnySource = Source<any>;
}

class SourceProxy<VALUE> extends Source<VALUE> {
  constructor(private $consumable: Consumable<VALUE>) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$consumable.consume(handler, options);
  }
}
