import { Stream, ErrorStream } from "../../streams/index.ts";

const NAME = "map";

export class Map<
  SOURCE extends Stream<any, any>,
  MAPPED = Stream.ExtractCleanValueFromSource<SOURCE>,
  ERROR = never,
  NAME extends string = map.Name,
> extends ErrorStream<
  | MAPPED
  | Stream.ExtractSentinelFromSource<SOURCE>
  | ErrorStream.MaybeSourceErr<ERROR, ErrorStream.SourceErr<Map<SOURCE, MAPPED, ERROR, NAME>>>,
  ERROR,
  NAME
> {
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    mapper: map.Mapper<SOURCE, MAPPED, ERROR, Map<SOURCE, MAPPED, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        const rawValue = value as Stream.ExtractCleanValueFromValue<Stream.ExtractValueFromSource<SOURCE>>;
        try {
          const maybePromise = mapper(rawValue, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (ErrorStream.isErr(result)) {
            self._errors?.push({ type: "expected", source: self, value: rawValue, detail: result.value });

            yield ErrorStream.sourceErr({
              source: self,
              value: value,
              detail: result.value,
            }) as never;

            continue;
          }

          yield result;
        } catch (error) {
          self._errors?.push({ type: "unexpected", source: self, value: rawValue, detail: error });

          yield Stream.sourceErr({
            source: self,
            value: value,
            detail: error,
          }) as never;
        }
      }
    });

    const self = this;
  }
}

export function map<
  SOURCE extends Stream<any, any>,
  MAPPED = Stream.ExtractValueFromSource<SOURCE>,
  ERROR = never,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<SOURCE, MAPPED, ERROR, Map<SOURCE, MAPPED, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Map<SOURCE, MAPPED, ERROR, NAME>> {
  return (_, source, name) => new Map(source, name, mapper);
}

export namespace map {
  export type Name = typeof NAME;

  export type Mapper<SOURCE extends Stream<any, any>, MAPPED, ERROR, SELF extends Stream<any, any>> = (
    value: Stream.ExtractCleanValueFromSource<SOURCE>,
    self: SELF,
  ) => MAPPED | ErrorStream.Err<ERROR> | Promise<MAPPED | ErrorStream.Err<ERROR>>;
}
