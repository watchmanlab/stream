import { Stream } from "../../stream-0";
import { filter } from "../filter/filter";
import { sequential } from "../map/map";
import { statefull } from "../statefull";
import { takeWhile } from "../take-while";

/**
 * Take first N values
 *
 * @example
 * ```typescript
 * stream.pipe(take(5))
 * ```
 */
export const take =
  <T>(n: number): Stream.Transformer<Stream<T>, Stream<T>> =>
  (stream) =>
    stream.pipe(takeWhile((_, index) => index < n));
