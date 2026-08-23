import { Consumer } from "./consumer";

export interface Consumable<VALUE> {
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
}

export namespace Consumable {
  export type AnyConsumable = Consumable<any>;
  export interface Consume<VALUE> {
    (handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
  }
  export function isConsumable<T>(object: unknown): object is Consumable<T> {
    return typeof object === "object" && object !== null && "consume" in object && typeof object.consume === "function";
  }
}
