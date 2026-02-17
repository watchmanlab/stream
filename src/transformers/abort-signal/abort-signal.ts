import { Stream, Controller } from "../../streams/stream/stream";
import { merge } from "../merge";

export function abortSignal<VALUE>(signal: AbortSignal): Stream.Transformer<Stream<VALUE>, Stream<Controller.Aborted>> {
  return function (source) {
    return new Stream(async function* () {
      for await (const _ of source) {
        yield Controller.ABORTED;
      }
    }).pipe(
      merge(
        new Stream(async function* () {
          try {
            if (signal.aborted) yield Controller.ABORTED;
            yield new Promise<Controller.Aborted>((resolve) =>
              signal.addEventListener("abort", () => resolve(Controller.ABORTED), { once: true }),
            );
          } finally {
          }
        }),
      ),
    );
  };
}
