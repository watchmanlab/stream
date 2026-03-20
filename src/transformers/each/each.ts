import { Stream } from "../../streams/index.ts";
import { Map } from "../map/map.ts";

const NAME = "each";

export class Each<
  SOURCE extends Stream<any, any>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  ERROR = never,
  NAME extends string = each.Name,
> extends Map<SOURCE, SELF, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME> {
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    callback: each.Callback<CLEAN_VALUE, ERROR, Each<SOURCE, SELF, CLEAN_VALUE, ERROR, NAME>>,
  ) {
    super(source, name, async (value) => {
      const maybePromise = callback(value, self);
      const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

      return result ?? value;
    });

    const self = this;
  }
}
export function each<
  SOURCE extends Stream<any, any>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  ERROR = never,
  NAME extends string = each.Name,
>(
  callback: each.Callback<CLEAN_VALUE, ERROR, Each<SOURCE, SELF, CLEAN_VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Each<SOURCE, SELF, CLEAN_VALUE, ERROR, NAME>> {
  return (_, source, name) => new Each(source, name, callback);
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<CLEAN_VALUE, ERROR, SELF extends Stream<any, any>> = (
    value: CLEAN_VALUE,
    self: SELF,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}
