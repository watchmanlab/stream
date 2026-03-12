import { Stream } from "../../streams";
import { Map } from "../map";

const NAME = "each";

export class Each<VALUE, ERROR = never, NAME extends string = each.Name> extends Stream<
  VALUE | Stream.MaybeSourceErr<ERROR, Stream.SourceErr<Stream.SafeValueOf<VALUE>, ERROR, Each<VALUE, ERROR, NAME>>>,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<Stream.SafeValueOf<VALUE>, ERROR, this>, `${NAME}Errors`>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSourceErr(value)) {
          yield value;
          continue;
        }

        const rawValue = value as Stream.SafeValueOf<VALUE>;
        try {
          const maybePromise = callback(rawValue, self);
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

          yield rawValue;
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
export function each<VALUE, ERROR = never, NAME extends string = each.Name>(
  callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Each<VALUE, ERROR, NAME>> {
  return (_, source, name) => new Each(source, name, callback);
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<VALUE, ERROR, SELF extends Stream<any, any>> = (
    value: Stream.SafeValueOf<VALUE>,
    self: SELF,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}
