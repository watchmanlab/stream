import { Consumer } from "./consumer";

/**
 * Represents any object that can create a {@link Consumer} .
 * The fundamental interface of the stream engine — anything with a `consume` method
 * participates in the credit-driven pipeline.
 *
 * @template VALUE The type of values emitted.
 */
export interface Consumable<VALUE> {
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
}

export namespace Consumable {
  export type AnyConsumable = Consumable<any>;

  export interface Consume<VALUE> {
    (handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
  }
}
