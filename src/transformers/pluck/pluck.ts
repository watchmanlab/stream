import { Stream } from "../../stream";
import { map } from "../sequential";

/**
 * Extract object property
 *
 * @example
 * ```typescript
 * stream.pipe(pluck("name"))
 * ```
 */

export function pluck<VALUE extends object, KEY extends keyof VALUE>(
  key: KEY,
): Stream.Transformer<Stream<VALUE>, Stream<VALUE[KEY]>> {
  return function (stream) {
    return stream.pipe(map((value) => value[key]));
  };
}
