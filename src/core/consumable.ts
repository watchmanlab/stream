import { Consumer } from "./consumer";

export interface Consumable<VALUE> {
  consume: Consumable.Consume<VALUE>;
}

export namespace Consumable {
  export type AnyConsumable = Consumable<any>;
  export type Consume<VALUE> = (handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>) => Consumer<VALUE>;
  export type ConsumableLike<VALUE> = Consumable<VALUE> | Consume<VALUE>;
  export type AnyConsumableLike = ConsumableLike<any>;
  export function isConsumable<T>(object: unknown): object is Consumable<T> {
    return typeof object === "object" && object !== null && "consume" in object && typeof object.consume === "function";
  }
}
