import { Stream } from "../../streams/index.ts";

const NAME = "map";

export class Map<VALUE, MAPPED = VALUE, ERROR = unknown, NAME extends string = map.Name> extends Stream<MAPPED, NAME> {
  protected _errors?: Stream<map.ErrorEvent<ERROR, this>, `${NAME}Errors`>;

  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    mapper: map.Mapper<VALUE, MAPPED, ERROR, Map<VALUE, MAPPED, ERROR, NAME>>,
  ) {
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
                const len = compensations.length;
                for (let i = 0; i < len; i++) {
                  await compensations[i](feedback);
                }
              }

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
  export type Compensate = (fn: (error: Stream.Result.SourceErr) => void | Promise<void>) => void;
  export type Mapper<VALUE, MAPPED, ERROR, SELF extends Stream<MAPPED, any>> = (
    value: VALUE,
    self: SELF,
    compensate: Compensate,
  ) => MAPPED | Stream.Result.Err<ERROR> | Promise<MAPPED | Stream.Result.Err<ERROR>>;
  export type ErrorEvent<ERROR, SELF extends Stream<any, any>> =
    | { type: "expected"; error: ERROR; self: SELF }
    | { type: "unexpected"; error: unknown; self: SELF };
}
