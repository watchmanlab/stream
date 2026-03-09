import { Stream } from "../../streams/index.ts";
import { each } from "../each/each.ts";

const NAME = "map";

export class Map<VALUE, MAPPED = VALUE, ERROR = unknown, NAME extends string = map.Name> extends Stream<
  map.Mapped<VALUE, MAPPED, ERROR, NAME>,
  NAME
> {
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    mapper: map.Mapper<VALUE, MAPPED, ERROR, Map<VALUE, MAPPED, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        try {
          if (Stream.Result.isSourceErr(value)) {
            yield value as never;
            continue;
          }
          const maybePromise = mapper(value, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.Result.isErr(result)) {
            yield Stream.Result.sourceErr({
              source: self,
              error: result.value,
              value: value as Stream.RawValueOf<typeof self>,
            }) as never;
            continue;
          }

          yield result;
        } catch (error: any) {
          yield Stream.Result.sourceErr({
            source: self,
            error,
            value: value as Stream.RawValueOf<typeof self>,
          }) as never;
        }
      }
    });

    const self = this;
  }
}

export function map<VALUE, MAPPED = VALUE, ERROR = unknown, NAME extends string = map.Name>(
  mapper: map.Mapper<VALUE, MAPPED, ERROR, Map<VALUE, MAPPED, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Map<VALUE, MAPPED, ERROR, NAME>> {
  return (_, source, name) => new Map(source, name, mapper);
}

export namespace map {
  export type Name = typeof NAME;
  export type Mapped<VALUE, MAPPED, ERROR = undefined, NAME extends string = Name> = undefined extends ERROR
    ? MAPPED | Stream.SourceErrorOf<VALUE>
    : MAPPED | Stream.SourceErrorOf<VALUE> | Stream.Result.SourceErr<Map<VALUE, MAPPED, ERROR, NAME>, ERROR>;

  export type Mapper<VALUE, MAPPED, ERROR, SELF extends Stream<any, any>> = (
    value: VALUE,
    self: SELF,
  ) => MAPPED | Stream.Result.Err<ERROR> | Promise<MAPPED | Stream.Result.Err<ERROR>>;
}

const stream = new Stream([1, 2, 3])
  .pipe(
    "map1",
    map((v) => v.toFixed()),
  )
  .pipe(
    "map2",
    map((v) => (v === "kechma" ? true : Stream.Result.err("kechmahaja" as const))),
  )
  .pipe(
    each((v) => {
      if (v === true) return Stream.Result.err("mmmm" as const);
      console.log(v);
    }),
  )
  .pipe(
    each((v) => {
      v;
    }),
  );
