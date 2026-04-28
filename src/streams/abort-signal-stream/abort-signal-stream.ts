//@ts-nocheck
import { Stream } from "../stream";

const NAME = "abort-signal-stream";

export class AbortSignalStream<NAME extends string = AbortSignalStream.Name> extends Stream<
  AbortSignalStream.Signal,
  NAME
> {
  protected _aborted = false;
  constructor(signal: AbortSignal, name = NAME as NAME) {
    let abortController: AbortController;

    super(name, async function* () {
      try {
        if (signal.aborted) yield AbortSignalStream.SIGNAL;
        yield new Promise<AbortSignalStream.Signal>((resolve) => {
          abortController = new AbortController();
          signal.addEventListener("abort", () => resolve(AbortSignalStream.SIGNAL), {
            once: true,
            signal: abortController.signal,
          });
        });
      } finally {
        self._aborted = true;
        abortController.abort();
        return;
      }
    });
    const self = this;
  }

  get aborted() {
    return this._aborted;
  }
}

export namespace AbortSignalStream {
  export type Name = typeof NAME;
  export const SIGNAL = Symbol(`*${NAME}#`);
  export type Signal = typeof SIGNAL;
}
