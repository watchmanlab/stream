import { Stream } from "../../streams";
import { Map } from "../map";

const NAME = "each";

export class Each<VALUE, ERROR = never, NAME extends string = each.Name> extends Stream<
  VALUE | Stream.MaybeSourceErr<ERROR, Stream.SourceErr<Stream.RawValueOf<VALUE>, ERROR, Each<VALUE, ERROR, NAME>>>,
  NAME
> {
  protected _map: Map<VALUE, VALUE, ERROR, NAME>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of self._map) {
        if (Stream.isSourceErr(value)) {
          yield Stream.sourceErr({ ...value, source: self }) as never;
          continue;
        }
        yield value;
      }
    });

    const self = this;

    this._map = new Map(source, NAME as NAME, async (value, _) => {
      const maybePromise = callback(value, self);

      const error = maybePromise instanceof Promise ? await maybePromise : maybePromise;

      if (Stream.isErr(error)) return error;

      return value;
    });
  }

  get errors() {
    return this._map.errors;
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
    value: Stream.RawValueOf<VALUE>,
    self: SELF,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}
