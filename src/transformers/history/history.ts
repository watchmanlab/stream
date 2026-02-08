import { statefull } from "../statefull";
import { Stream } from "../../stream";

/**
 * Emit array of last N values (sliding window)
 *
 * @example
 * ```typescript
 * stream.pipe(history(3))
 * // 1 → [1]
 * // 2 → [1, 2]
 * // 3 → [1, 2, 3]
 * // 4 → [2, 3, 4]
 * ```
 */

export function history<VALUE>(size: number): Stream.Transformer<Stream<VALUE>, Stream<VALUE[]>> {
  return function (source) {
    return new Stream((self) => {
      let window: VALUE[] = [];

      return source
        .listen((value) => {
          window = [...window, value].slice(-size);
          self.push(window);
        })
        .addCleanup(() => (window.length = 0));
    });
  };
}
