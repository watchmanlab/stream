import { Stream } from "../../streams/index.ts";

const NAME = "map";

export class Map<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
> extends Stream<
  | MAPPED
  | Stream.ExtractSentinel<SOURCE>
  | Stream.MaybeSourceErr<CLEAN_VALUE, ERROR, Map<SOURCE, CLEAN_VALUE, MAPPED, ERROR, NAME>>,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR, this>, `${NAME}Errors`>;

  constructor(
    source: SOURCE,
    name = NAME as NAME,
    mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR, Map<SOURCE, CLEAN_VALUE, MAPPED, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        const rawValue = value as CLEAN_VALUE;
        try {
          const maybePromise = mapper(rawValue, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._errors?.push({
              type: "expected",
              value: rawValue,
              detail: result.value,
              source: self,
            });

            yield Stream.sourceErr({
              value: rawValue,
              detail: result.value,
              source: self,
            }) as never;

            continue;
          }

          yield result;
        } catch (error) {
          self._errors?.push({ type: "unexpected", source: self, value: rawValue, detail: error });

          yield Stream.sourceErr({
            value: value,
            detail: error,
            source: self,
          }) as never;
        }
      }
    });

    const self = this;
  }
  get errors() {
    if (!this._errors) this._errors = new Stream(`${this._name}Errors` as never);
    return this._errors;
  }
}

export function map<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR, Map<SOURCE, CLEAN_VALUE, MAPPED, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Map<SOURCE, CLEAN_VALUE, MAPPED, ERROR, NAME>> {
  return (_, source, name) => new Map(source, name, mapper);
}

export namespace map {
  export type Name = typeof NAME;

  export type Mapper<CLEAN_VALUE, MAPPED, ERROR, SELF extends Stream<any, any>> = (
    value: CLEAN_VALUE,
    self: SELF,
  ) => MAPPED | Stream.Err<ERROR> | Promise<MAPPED | Stream.Err<ERROR>>;
}
