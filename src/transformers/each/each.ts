import { Stream } from "../../streams";

const NAME = "each";

export class Each<VALUE, ERROR, NAME extends string = each.Name> extends Stream<
  undefined extends ERROR ? VALUE : VALUE | Stream.Result.SourceErr<Each<VALUE, ERROR, NAME>, ERROR>,
  NAME
> {
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        try {
          if (Stream.Result.isSourceErr(value)) {
            yield value;
            continue;
          }

          const maybePromise = callback(value, self);

          const error = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.Result.isErr(error)) {
            yield Stream.Result.sourceErr({
              source: self,
              error: error.value,
              value: value as Stream.RawValueOf<typeof self>,
            }) as never;
            continue;
          }

          yield value;
        } catch (error) {
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
export function each<VALUE, ERROR, NAME extends string = each.Name>(
  callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Each<VALUE, ERROR, NAME>> {
  return (_, source, name) => new Each(source, name, callback);
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<VALUE, ERROR, SELF extends Stream<any, any>> = (
    value: VALUE,
    self: SELF,
  ) => void | Stream.Result.Err<ERROR> | Promise<void | Stream.Result.Err<ERROR>>;
}
