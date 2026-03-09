import { Stream } from "../../streams/index.ts";
import { each } from "../each/each.ts";

const NAME = "map";

export class Map<VALUE, MAPPED = VALUE, ERROR = unknown, NAME extends string = map.Name> extends Stream<MAPPED, NAME> {
  protected _errors?: Stream<Stream.ErrorEvent<VALUE, ERROR, this>, `${NAME}Errors`>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    mapper: map.Mapper<VALUE, MAPPED, ERROR, Map<VALUE, MAPPED, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.Result.isSourceErr(value)) {
          yield value as MAPPED;
          continue;
        }

        try {
          const maybePromise = mapper(value, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.Result.isErr(result)) {
            self._errors?.push({ type: "expected", source: self, value, detail: result.value });

            yield Stream.Result.sourceErr({
              source: self,
              value: value,
              detail: result.value,
            }) as MAPPED;

            continue;
          }

          yield result;
        } catch (error) {
          self._errors?.push({ type: "unexpected", source: self, value: value, detail: error });

          yield Stream.Result.sourceErr({
            source: self,
            value: value,
            detail: error,
          }) as MAPPED;
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

export function map<VALUE, MAPPED = VALUE, ERROR = unknown, NAME extends string = map.Name>(
  mapper: map.Mapper<VALUE, MAPPED, ERROR, Map<VALUE, MAPPED, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Map<VALUE, MAPPED, ERROR, NAME>> {
  return (_, source, name) => new Map(source, name, mapper);
}

export namespace map {
  export type Name = typeof NAME;

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
