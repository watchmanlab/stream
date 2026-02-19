import { Stream } from "../../stream";

const NAME = "abort-signal";
type Name = typeof NAME;

export class AbortSignal extends Stream<AbortSignal.Signal, Name> {
  protected _aborted = false;
  constructor(signal: globalThis.AbortSignal) {
    super(NAME, async function* () {
      try {
        if (signal.aborted) yield AbortSignal.SIGNAL;
        yield new Promise<AbortSignal.Signal>((resolve) =>
          signal.addEventListener("abort", () => resolve(AbortSignal.SIGNAL), { once: true }),
        );
      } finally {
        self._aborted = true;
        return;
      }
    });
    const self = this;
  }

  get aborted() {
    return this._aborted;
  }
}

export namespace AbortSignal {
  export const SIGNAL = Symbol("*abort-signal#");
  export type Signal = typeof SIGNAL;
}
