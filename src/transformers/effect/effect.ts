import { Stream } from "../../streams";

const NAME = "effect";

export class Effect<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  ERROR = never,
  NAME extends string = effect.Name,
> extends Stream<
  Stream.ExtractValue<SOURCE> | Stream.MaybeSourceErr<CLEAN_VALUE, ERROR, Effect<SOURCE, CLEAN_VALUE, ERROR, NAME>>,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR, this>, `${NAME}Errors`>;

  constructor(
    source: SOURCE,
    name = NAME as NAME,
    callback: effect.Callback<CLEAN_VALUE, ERROR, Effect<SOURCE, CLEAN_VALUE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSentinel(value)) {
          yield value;
          continue;
        }

        const rawValue = value as CLEAN_VALUE;

        try {
          const maybePromise = callback(rawValue, self);

          if (maybePromise instanceof Promise) {
            maybePromise
              .then((error) => {
                if (error) self._errors?.push({ type: "expected", source: self, value: rawValue, detail: error.value });
              })
              .catch((error) => {
                self._errors?.push({ type: "unexpected", source: self, value: rawValue, detail: error });
              });
          }

          if (Stream.isErr(maybePromise)) {
            self._errors?.push({ type: "expected", source: self, value: rawValue, detail: maybePromise.value });
          }
        } catch (error) {
          self._errors?.push({ type: "unexpected", source: self, value: rawValue, detail: error });
        } finally {
          yield value;
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
export function effect<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  ERROR = never,
  NAME extends string = effect.Name,
>(
  callback: effect.Callback<CLEAN_VALUE, ERROR, Effect<SOURCE, CLEAN_VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Effect<SOURCE, CLEAN_VALUE, ERROR, NAME>> {
  return (_, source, name) => new Effect(source, name, callback);
}
export namespace effect {
  export type Name = typeof NAME;

  export type Callback<CLEAN_VALUE, ERROR, SELF extends Stream<any, any>> = (
    value: CLEAN_VALUE,
    self: SELF,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}
