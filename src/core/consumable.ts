import { Consumer } from "./consumer";

export interface Consumable<T> {
  consume: Consumable.Consume<T>;
}

export namespace Consumable {
  export type AnyConsumable = Consumable<any>;
  export type Consume<T> = <C extends Consumer<T>>(consumer: C) => C;

  export function isConsumable<T>(object: unknown): object is Consumable<T> {
    return typeof object === "object" && object !== null && "consume" in object && typeof object.consume === "function";
  }
}
