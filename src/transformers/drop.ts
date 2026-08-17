import { AnyConsumable } from "../core/types";
import { dropNewest } from "./drop-newest";
import { dropOldest } from "./drop-oldest";

export function drop<INPUT extends AnyConsumable>(maxSize: number, strategy: "oldest" | "newest") {
  return strategy === "oldest" ? dropOldest<INPUT>(maxSize) : dropNewest<INPUT>(maxSize);
}
