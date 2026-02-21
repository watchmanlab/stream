import { Stream } from "../../streams";
import { consumer } from "../consumer";

const NAME = "delay";

export class Delay<VALUE, NAME extends string = delay.Name> extends Stream<VALUE, NAME> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, ms: number) {
    super(name, async function* () {
      let timer: any = null;
      let aborted = false;
      let resolver: () => void;

      try {
        for await (const value of source) {
          if (aborted) break;
          await new Promise<void>((r) => {
            resolver = r;
            timer = setTimeout(() => r(), ms);
          });

          if (aborted) break;

          yield value;
        }
      } finally {
        resolver!?.();
        aborted = true;
      }
    });
  }
}

export function delay<VALUE, NAME extends string = delay.Name>(
  ms: number,
): Stream.Transformer<NAME, Stream<VALUE, any>, Delay<VALUE, NAME>> {
  return (_, source, name) => new Delay(source, name, ms);
}

export namespace delay {
  export type Name = typeof NAME;
}
