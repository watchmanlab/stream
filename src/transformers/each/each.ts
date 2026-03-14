import { Stream } from "../../streams/index.ts";

const NAME = "each";

export class Each<SOURCE extends Stream<any, any>, ERROR = never, NAME extends string = each.Name> extends Stream<
  | Stream.ExtractValueFromSource<SOURCE>
  | Stream.MaybeSourceErr<
      ERROR,
      Stream.SourceErr<Stream.ExtractCleanValueFromSource<SOURCE>, ERROR, Each<SOURCE, ERROR, NAME>>
    >,
  NAME
> {
  protected _errors?: Stream<
    Stream.ErrorEvent<Stream.ExtractCleanValueFromSource<SOURCE>, ERROR, this>,
    `${NAME}Errors`
  >;
  constructor(source: SOURCE, name = NAME as NAME, callback: each.Callback<SOURCE, ERROR, Each<SOURCE, ERROR, NAME>>) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSourceErr(value)) {
          yield value as never;
          continue;
        }

        const rawValue = value as Stream.ExtractCleanValueFromSource<SOURCE>;
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
export function each<SOURCE extends Stream<any, any>, ERROR = never, NAME extends string = each.Name>(
  callback: each.Callback<Stream.ExtractValueFromSource<SOURCE>, ERROR, Each<SOURCE, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Each<SOURCE, ERROR, NAME>> {
  return (_, source, name) => new Each(source, name, callback);
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<SOURCE extends Stream<any, any>, ERROR, SELF extends Stream<any, any>> = (
    value: Stream.ExtractCleanValueFromSource<SOURCE>,
    self: SELF,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}
