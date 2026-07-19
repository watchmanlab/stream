import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { fromIterator } from "./from-iterator";

export function fromGenerator<VALUE, NAME extends NonEmptyString = "root">(
  functionGenerator: () => Generator<VALUE>,
  options?: Stream.Options<VALUE, NAME>,
) {
  return fromIterator(functionGenerator, options);
}
