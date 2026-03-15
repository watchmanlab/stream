import { Stream } from "../../streams/index.ts";

const NAME = "map";

export class Map<
  SOURCE extends Stream<any, any>,
  MAPPED = Stream.ExtractCleanValue<SOURCE>,
  NAME extends string = map.Name,
> extends Stream<
  | Stream.ExtractCleanValue<MAPPED>
  | Stream.ExtractSentinel<SOURCE>
  | Stream.MaybeSourceErr<Map<SOURCE, MAPPED, NAME>, Stream.ExtractCleanValue<SOURCE>, Stream.ExtractError<MAPPED>>,
  NAME
> {
  protected _errors?: Stream<
    Stream.ErrorEvent<this, Stream.ExtractCleanValue<SOURCE>, Stream.ExtractError<MAPPED>>,
    `${NAME}Errors`
  >;

  constructor(source: SOURCE, name = NAME as NAME, mapper: map.Mapper<SOURCE, MAPPED, Map<SOURCE, MAPPED, NAME>>) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSentinel(value)) {
          yield value as Stream.ExtractSentinel<SOURCE>;
          continue;
        }

        const rawValue = value as Stream.ExtractCleanValue<SOURCE>;
        try {
          const maybePromise = mapper(rawValue, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr<Stream.ExtractError<MAPPED>>(result)) {
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

          yield result as never;
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
  MAPPED = Stream.ExtractCleanValue<SOURCE>,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<SOURCE, MAPPED, Map<SOURCE, MAPPED, NAME>>,
): Stream.Transformer<NAME, SOURCE, Map<SOURCE, MAPPED, NAME>> {
  return (_, source, name) => new Map(source, name, mapper);
}

export namespace map {
  export type Name = typeof NAME;

  export type Mapper<SOURCE extends Stream<any, any>, MAPPED, SELF extends Stream<any, any>> = (
    value: Stream.ExtractCleanValue<SOURCE>,
    self: SELF,
  ) => MAPPED | Promise<MAPPED>;
}

const stream = new Stream([1, 2, 3, 4])
  .pipe(
    map((v) => {
      if (v === 3) return Stream.err("kechmahaja" as const);
      return v.toFixed();
    }),
  )
  .pipe(map((v) => v));

stream.errors.next().then((r) => {
  if (r.done) return;
  if (r.value.type === "expected") {
    r.value.detail;
  }
});
type S = Stream.ExtractValue<typeof stream>;
//.  ^?
