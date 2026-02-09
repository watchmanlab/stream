import { Stream } from "../../stream-0";
import { derive } from "../derive";

/**
 * Temporarily HOT transformer that captures values pushed before listener is added.
 *
 * **Two phases:**
 * 1. HOT phase (first microtask): Captures all values pushed synchronously
 * 2. COLD phase (after first microtask): Becomes direct passthrough
 *
 * @see {@link https://github.com/soffinal/stream/blob/main/src/transformers/microbatch/microbatch.md} - Full documentation
 *
 * @example
 * ```typescript
 * const source = new Stream<number>();
 * const batched = source.pipe(microbatch());
 *
 * // Push before listener exists
 * source.push(1, 2, 3);
 *
 * // Listener receives all values
 * batched.listen(v => console.log(v)); // 1, 2, 3
 * ```
 *
 * @example
 * ```typescript
 * // Initialization pattern
 * const config = new Stream<Config>();
 * const settings = config.pipe(microbatch());
 *
 * config.push({ theme: 'dark' });
 * config.push({ lang: 'en' });
 *
 * settings.listen(cfg => applyConfig(cfg));
 * ```
 */
export function microbatch<VALUE>(): Stream.Transformer<Stream<VALUE>> {
  return function (source) {
    const queue: VALUE[] = [];

    const output = source.pipe(derive());

    // HOT phase: Listen immediately to capture early pushes
    let controller = source.listen((value) => {
      queue.push(value);
    });

    // Abort HOT listener after first microtask
    queueMicrotask(() => {
      controller.abort();
      if (queue.length) {
        output.push(...(queue as [VALUE, ...VALUE[]]));
        queue.length = 0;
      }
    });

    return output;
  };
}
