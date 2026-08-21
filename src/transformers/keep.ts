import { keepOldest } from "./keep-oldest";
import { keepNewest } from "./keep-newest";
import { Consumable } from "../core/consumable";

export function keep<INPUT extends Consumable.AnyConsumable>(maxSize: number, strategy: "oldest" | "newest") {
  return strategy === "newest" ? keepNewest<INPUT>(maxSize) : keepOldest<INPUT>(maxSize);
}
