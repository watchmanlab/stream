import { Stream } from "../../streams/stream/stream-0";

/**
 * Emit first value, then ignore until quiet period
 *
 * @example
 * ```typescript
 * stream.pipe(audit(300)) // Emit first, ignore for 300ms
 * ```
 */

export function audit<T>(ms: number): Stream.Transformer<Stream<T>> {
  return function (source) {
    return new Stream<T>(async function* () {
      let timer: any = null;
      let canEmit = true;

      try {
        for await (const value of source) {
          if (canEmit) {
            canEmit = false;
            clearTimeout(timer);
            timer = setTimeout(() => (canEmit = true), ms!);
            yield value;
          }
        }
      } finally {
        clearTimeout(timer);
      }
    });
  };
}
