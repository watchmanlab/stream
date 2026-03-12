import { Stream } from "../../streams/index.ts";
import { each } from "../each/each.ts";

const NAME = "map";

export class Map<VALUE, MAPPED = VALUE, ERROR = never, NAME extends string = map.Name> extends Stream<
  | MAPPED
  | Stream.SourceErrOf<VALUE>
  | Stream.MaybeSourceErr<ERROR, Stream.SourceErr<Stream.RawValueOf<VALUE>, ERROR, Map<VALUE, MAPPED, ERROR, NAME>>>,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<Stream.RawValueOf<VALUE>, ERROR, this>, `${NAME}Errors`>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    mapper: map.Mapper<VALUE, MAPPED, ERROR, Map<VALUE, MAPPED, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSourceErr(value)) {
          yield value as never;
          continue;
        }

        const rawValue = value as Stream.RawValueOf<VALUE>;
        try {
          const maybePromise = mapper(rawValue, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._errors?.push({ type: "expected", source: self, value: rawValue, detail: result.value });

            yield Stream.sourceErr({
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

  get errors() {
    if (!this._errors) this._errors = new Stream(`${this._name}Errors` as never);
    return this._errors;
  }
}

export function map<VALUE, MAPPED = VALUE, ERROR = never, NAME extends string = map.Name>(
  mapper: map.Mapper<VALUE, MAPPED, ERROR, Map<VALUE, MAPPED, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Map<VALUE, MAPPED, ERROR, NAME>> {
  return (_, source, name) => new Map(source, name, mapper);
}

export namespace map {
  export type Name = typeof NAME;

  export type Mapper<VALUE, MAPPED, ERROR, SELF extends Stream<any, any>> = (
    value: Stream.RawValueOf<VALUE>,
    self: SELF,
  ) => MAPPED | Stream.Err<ERROR> | Promise<MAPPED | Stream.Err<ERROR>>;
}

const stream = new Stream([1, 2, 3])
  .pipe(
    "map1",
    map((v) => v.toFixed()),
  )
  .pipe(
    "map2",
    map((v) => (v === "kechma" ? true : Stream.err("kechmahaja" as const))),
  )
  .pipe(
    "each1",
    each((v) => {
      if (v === true) return Stream.err("mmmm" as const);
      console.log(v);
    }),
  )
  .pipe(
    "each2",
    each((v) => {
      v;
    }),
  );

for await (const value of stream) {
  if (value instanceof Stream.SourceErr) {
    // switch (value.name) {
    //   case "each1":
    //     value.source.push(true);
    //     break;
    //   case "map1":
    //     break;
    //   case "map2":
    //     break;
    // }
  }
}
