import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class AbortSignalStream<NAME extends NonEmptyString = "$abortSignal"> extends Stream<void, NAME> {
  constructor(
    public readonly signal: AbortSignal,
    options?: Stream.Options<void, NAME>,
  ) {
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
