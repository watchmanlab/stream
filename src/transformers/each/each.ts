import { Stream } from "../../streams";
import { catchError } from "../catch-error";
import { map, Map } from "../map";
import { pump } from "../pump";

const NAME = "each";

export class Each<VALUE, ERROR, NAME extends string = each.Name> extends Map<VALUE, VALUE, ERROR, NAME> {
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
  ) {
    super(source, name, async (value, self, compensate) => {
      await callback(value, self, compensate);
      return value;
    });
  }
}
export function each<VALUE, ERROR, NAME extends string = each.Name>(
  callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Each<VALUE, ERROR, NAME>> {
  return (_, source, name) => new Each(source, name, callback);
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<VALUE, ERROR, SELF extends Each<VALUE, ERROR, any>> = (
    value: VALUE,
    self: SELF,
    compensate: map.Compensate,
  ) => void | Stream.Result.Err<ERROR> | Promise<void | Stream.Result.Err<ERROR>>;
}
