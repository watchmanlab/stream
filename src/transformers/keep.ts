import { AnyConsumable } from "../core/types";
import { keepOldest } from "./keep-oldest";
import { keepNewest } from "./keep-newest";

export function keep<INPUT extends AnyConsumable>(maxSize: number, strategy: "oldest" | "newest") {
  return strategy === "newest" ? keepNewest<INPUT>(maxSize) : keepOldest<INPUT>(maxSize);
}
