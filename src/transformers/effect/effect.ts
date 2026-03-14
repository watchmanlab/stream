import { Stream } from "../../streams";

const NAME = "effect";

export class Effect<VALUE, ERROR = never, NAME extends string = effect.Name> extends Stream<VALUE, NAME> {
  protected _errors?: Stream<Stream.ErrorEvent<Stream.ExtractCleanValueFromValue<VALUE>, ERROR, this>, `${NAME}Errors`>;

  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback: effect.Callback<VALUE, ERROR, NAME>) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSourceErr(value)) {
          yield value;
          continue;
        }

        const rawValue = value as Stream.ExtractCleanValueFromValue<VALUE>;

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
export function effect<VALUE, ERROR = never, NAME extends string = effect.Name>(
  callback: effect.Callback<VALUE, ERROR, NAME>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Effect<VALUE, ERROR, NAME>> {
  return (_, source, name) => new Effect(source, name, callback);
}
export namespace effect {
  export type Name = typeof NAME;

  export type Callback<VALUE, ERROR, NAME extends string> = (
    value: Stream.ExtractCleanValueFromValue<VALUE>,
    self: Effect<VALUE, ERROR, NAME>,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}
