import { Stream } from "../../stream-0";

/**
 * Take until notifier stream emits
 *
 * @example
 * ```typescript
 * stream.pipe(takeUntil(stopSignal))
 * ```
 */
export function takeUntil<T>(notifier: AbortSignal): Stream.Transforme<Stream<T>, Stream<T>>;
export function takeUntil<T>(notifier: object): Stream.Transforme<Stream<T>, Stream<T>>;
export function takeUntil<T>(notifier: Stream): Stream.Transforme<Stream<T>, Stream<T>>;
export function takeUntil<T>(notifier: AbortSignal | object | Stream): Stream.Transforme<Stream<T>, Stream<T>> {
  return (source) =>
    new Stream<T>(async function* () {
      const output = new Stream<T>();
      const controller = source.listen(output.push.bind(output), notifier);

      try {
        for await (const value of output) {
          if (controller.aborted) break;
          yield value;
        }
      } finally {
        controller.abort();
      }
    });
}
