import { Stream } from "../../streams/index.ts";

const NAME = "each";

export class Each<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  ERROR = never,
  NAME extends string = each.Name,
> extends Stream<
  Stream.ExtractValue<SOURCE> | Stream.MaybeSourceErr<CLEAN_VALUE, ERROR, Each<SOURCE, CLEAN_VALUE, ERROR, NAME>>,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR, this>, `${NAME}Errors`>;
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    callback: each.Callback<CLEAN_VALUE, ERROR, Each<SOURCE, CLEAN_VALUE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSourceErr(value)) {
          yield value as never;
          continue;
        }

        const cleanValue = value as CLEAN_VALUE;

        try {
          const maybePromise = callback(cleanValue, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr<ERROR>(result)) {
            self._errors?.push({ type: "expected", source: self, value: cleanValue, detail: result.value });

            yield Stream.sourceErr({
              source: self,
              value: value,
              detail: result.value,
            }) as never;

            continue;
          }

          yield cleanValue as never;
        } catch (error) {
          if (Stream.isErr<ERROR>(error)) {
            self._errors?.push({ type: "expected", source: self, value: cleanValue, detail: error.value });
            yield Stream.sourceErr({ value: value, detail: error.value, source: self }) as never;
          } else {
            self._errors?.push({ type: "unexpected", source: self, value: cleanValue, detail: error });
            yield Stream.sourceErr({ value: value, detail: error, source: self }) as never;
          }
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
export function each<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  ERROR = never,
  NAME extends string = each.Name,
>(
  callback: each.Callback<CLEAN_VALUE, ERROR, Each<SOURCE, CLEAN_VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Each<SOURCE, CLEAN_VALUE, ERROR, NAME>> {
  return (_, source, name) => new Each(source, name, callback);
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<CLEAN_VALUE, ERROR, SELF extends Stream<any, any>> = (
    value: CLEAN_VALUE,
    self: SELF,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}
