import { Stream } from "../../streams";
import { map, Map } from "../map";

const NAME = "each";

export class Each<VALUE, ERROR, NAME extends string = each.Name> extends Stream<VALUE, NAME> {
  protected _map: Map<VALUE, VALUE, ERROR, NAME>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      yield* self._map;
    });

    const self = this;

    this._map = new Map(source, NAME as NAME, async (value, _) => {
      const maybePromise = callback(value, self);

      const error = maybePromise instanceof Promise ? await maybePromise : maybePromise;

      if (Stream.Result.isErr(error)) return error;

      return value;
    });
  }

  get errors(): Stream<Stream.ErrorEvent<VALUE, ERROR, this>, `${NAME}Errors`> {
    return this._map.errors.pipe(
      `${this._name}Errors`,
      map((e) => ({ ...e, source: this })),
    );
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
