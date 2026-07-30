import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class FromAbortSignal<NAME extends NonEmptyString = "$abortSignal"> extends Stream<void, NAME> {
  constructor(signal: AbortSignal, options?: Stream.Options<void, NAME>) {
    const { name, terminate, ...rest } = options ?? {};

    let abortController = new AbortController();

    super({
      ...rest,
      name: name ?? ("$abortSignal" as NAME),
      terminate(stream, reason) {
        abortController.abort();
        terminate?.(stream, reason);
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
