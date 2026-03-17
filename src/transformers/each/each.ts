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

        const rawValue = value as CLEAN_VALUE;

        try {
          const maybePromise = callback(rawValue, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr<ERROR>(result)) {
            self._errors?.push({ type: "expected", source: self, value: rawValue, detail: result.value });

            yield Stream.sourceErr({
              source: self,
              value: value,
              detail: result.value,
            }) as never;

            continue;
          }

          yield rawValue as never;
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
