import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class FromAbortSignal<NAME extends NonEmptyString = "$abortSignal"> extends Stream<void, NAME> {
  constructor(signal: AbortSignal, options?: Stream.Options<void, NAME>) {
    let abortController = new AbortController();

    super({
      ...options,
      name: options?.name ?? ("$abortSignal" as NAME),
      terminate(stream, reason) {
        abortController.abort();
        options?.terminate?.(stream, reason);
      },
    });

    if (signal.aborted) this.terminate("complete");

    signal.addEventListener(
      "abort",
      () => {
        this.push();
        this.terminate("complete");
      },
      {
        signal: abortController.signal,
      },
    );
  }
}
export function fromAbortSignal<NAME extends NonEmptyString = "$abortSignal">(
  signal: AbortSignal,
  options?: Stream.Options<void, NAME>,
): FromAbortSignal<NAME> {
  return new FromAbortSignal(signal, options);
}
