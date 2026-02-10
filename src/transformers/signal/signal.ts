import { Stream } from "../../stream";

export function signal(): Stream.Transformer<Stream<any>, signal.CapableStream> {
  return function (source) {
    let fired = false;
    return Stream.create<signal.fired, signal.Capability>(
      async function* () {
        await source.next();
        yield signal.FIRED;
      },
      () => {
        return {
          signal: {
            get fired() {
              return fired;
            },
          },
        };
      },
    );
  };
}
export namespace signal {
  export const FIRED = Symbol("fired");
  export type fired = typeof FIRED;
  export type Signal = { readonly fired: boolean };
  export type Capability = { readonly signal: Signal };
  export type CapableStream = Stream.Capable<signal.fired, Capability>;
}
