import { Stream } from "../../streams/index.ts";
import { catchError } from "../catch-error";
import { effect } from "../effect";
import { pump } from "../pump/pump.ts";

const NAME = "map";

export class Map<VALUE, MAPPED = VALUE, ERROR = unknown, NAME extends string = map.Name> extends Stream<MAPPED, NAME> {
  protected _errors?: Stream<map.ErrorEvent<VALUE, MAPPED, ERROR, NAME>, `${NAME}-errors`>;

  constructor(source: Stream<VALUE, any>, name = NAME as NAME, mapper: map.Mapper<VALUE, MAPPED, ERROR, NAME>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let next = await generator.next();

      try {
        while (!next.done) {
          let compensations: Array<(err: Stream.Result.SourceErr) => void | Promise<void>> | undefined;
          try {
            const result = await mapper(next.value, self, (fn: (err: Stream.Result.SourceErr) => void) => {
              if (!compensations) compensations = [];
              compensations.unshift(fn);
            });

            if (Stream.Result.isErr(result)) {
              self._errors?.push({ type: "expected", error: result.value, self });
              next = await generator.next(
                Stream.Result.sourceErr({ error: result.value, source: self, value: next.value }),
              );
            } else {
              const feedback = yield result;
              if (Stream.Result.isSourceErr(feedback) && compensations) {
                for (let i = 0; i < compensations.length; i++) {
                  await compensations[i](feedback);
                }
              }
              // compensations = undefined;
              next = await generator.next(feedback);
            }
          } catch (error: any) {
            if (Stream.Result.isErr(error)) {
              self._errors?.push({ type: "expected", error: error.value as ERROR, self });
              next = await generator.next(
                Stream.Result.sourceErr({ error: error.value, source: self, value: next.value }),
              );
            } else {
              self._errors?.push({ type: "unexpected", error: error, self });
              next = await generator.next(Stream.Result.sourceErr({ error, source: self, value: next.value }));
            }
          }
        }
      } finally {
        await generator.return();
      }
    });

    const self = this;
  }
  get errors() {
    if (!this._errors) this._errors = new Stream(`${this._name}-errors` as never);
    return this._errors;
  }
}

export function map<VALUE, MAPPED = VALUE, ERROR = unknown, NAME extends string = map.Name>(
  mapper: map.Mapper<VALUE, MAPPED, ERROR, NAME>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Map<VALUE, MAPPED, ERROR, NAME>> {
  return (_, source, name) => new Map(source, name, mapper);
}

export namespace map {
  export type Name = typeof NAME;
  export type Compensate = (fn: (error: Stream.Result.SourceErr) => void | Promise<void>) => void;
  export type Mapper<VALUE, MAPPED, ERROR, NAME extends string> = (
    value: VALUE,
    self: Map<VALUE, MAPPED, ERROR, NAME>,
    compensate: Compensate,
  ) => MAPPED | Stream.Result.Err<ERROR> | Promise<MAPPED | Stream.Result.Err<ERROR>>;
  export type ErrorEvent<VALUE, MAPPED, ERROR, NAME extends string> =
    | { type: "expected"; error: ERROR; self: Map<VALUE, MAPPED, ERROR, NAME> }
    | { type: "unexpected"; error: unknown; self: Map<VALUE, MAPPED, ERROR, NAME> };
}

new Stream([1, 2, 4])
  .pipe(
    catchError((err) => {
      console.log(err.error);
    }),
  )

  .pipe(
    map((v, self, compensate) => {
      compensate((e) => console.log(e.error));

      return v;
    }),
  )
  .pipe(
    map((v) => {
      // if (v === 1) throw Stream.Result.err("kechmahaja");
      return v.toFixed();
    }),
  )
  .pipe(effect((v) => console.log(v)))
  .pipe(pump());
