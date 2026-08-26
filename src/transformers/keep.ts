import { AnyConsumable } from "../core/types";
import { keepOldest } from "./keep-oldest";
import { keepNewest } from "./buffer-latest";

export function keep<INPUT extends AnyConsumable>(maxSize: number, strategy: "oldest" | "newest") {
  return strategy === "newest" ? keepNewest<INPUT>(maxSize) : keepOldest<INPUT>(maxSize);
}
