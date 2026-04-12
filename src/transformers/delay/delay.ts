import { Stream } from "../../streams/index.ts";

const NAME = "delay";

export class Delay<SOURCE extends Stream<any, any>, NAME extends string = delay.Name> extends Stream<
  Stream.ExtractValue<SOURCE>,
  NAME
> {
  constructor(source: SOURCE, name = NAME as NAME, ms: number) {
    super(name, async function* () {
      let timer: any = null;
      let resolver: () => void = () => {};

      try {
        for await (const value of source) {
          if (!Stream.isSentinel(value))
            await new Promise<void>((r) => {
              resolver = r;
              timer = setTimeout(r, ms);
            });

          yield value;
        }
      } finally {
        clearInterval(timer);
        resolver();
      }
    });
  }
}

export function delay<SOURCE extends Stream<any, any>, NAME extends string = delay.Name>(
  ms: number,
): Stream.Transform<NAME, SOURCE, Delay<SOURCE, NAME>> {
  return (_, source, name) => new Delay(source, name, ms);
}

export namespace delay {
  export type Name = typeof NAME;
}
