import { Stream } from "../../streams";
import { consumer } from "../consumer";

const NAME = "debounce";

export class Debounce<VALUE, NAME extends string = debounce.Name> extends Stream<VALUE, NAME> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, ms: number) {
    super(name, async function* () {
      let timer: any = null;
      let aborted = false;

      let resolve: (value: VALUE) => void;

      (async () => {
        for await (const value of source) {
          clearTimeout(timer);
          if (aborted) {
            resolve!?.(value);
            break;
          }
          timer = setTimeout(() => resolve!?.(value), ms);
        }
      })();

      try {
        while (true) {
          yield await new Promise<VALUE>((r) => (resolve = r));
        }
      } finally {
        aborted = true;
      }
    });
  }
}

export function debounce<VALUE, NAME extends string = debounce.Name>(
  ms: number,
): Stream.Transformer<NAME, Stream<VALUE, any>, Debounce<VALUE, NAME>> {
  return (_, source, name) => new Debounce(source, name, ms);
}

export namespace debounce {
  export type Name = typeof NAME;
}
