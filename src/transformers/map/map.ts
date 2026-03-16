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
  | Stream.MaybeSourceErr<Map<SOURCE, CLEAN_VALUE, MAPPED, ERROR, NAME>, CLEAN_VALUE, ERROR>,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<this, CLEAN_VALUE, ERROR>, `${NAME}Errors`>;

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
              source: self,
              value: rawValue,
              detail: result.value,
            });

            yield Stream.sourceErr({
              source: self,
              value: rawValue,
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

const stream = new Stream([1, 2, 3, 4])
  .pipe(
    map((v) => {
      if (v === 3) return Stream.err("kechmahaja" as const);
      return v.toFixed();
    }),
  )
  .pipe(map((v) => v));

stream.map.errors.next().then((r) => {
  if (r.done) return;
  if (r.value.type === "expected") {
    r.value.detail;
  }
});
type S = Stream.ExtractValue<typeof stream>;
//.  ^?
