import { Stream } from "../../streams/index.ts";
import { FixedArray } from "../../types/index.ts";

const NAME = "history";

export class History<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  NAME extends string = history.Name,
> extends Stream<CLEAN_VALUE[] | Stream.ExtractSentinel<SOURCE>, NAME> {
  constructor(source: SOURCE, name = NAME as NAME, size: number) {
    super(name, async function* () {
      let window: CLEAN_VALUE[] = [];

      for await (const value of source) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }
        if (window.length === size) window.unshift();

        window.push(value);

        yield window;
      }
    });
  }
}

export function history<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  NAME extends string = history.Name,
>(size: number): Stream.Transform<NAME, SOURCE, History<SOURCE, CLEAN_VALUE, NAME>> {
  return (_, source, name) => new History(source, name, size);
}

export namespace history {
  export type Name = typeof NAME;
}
