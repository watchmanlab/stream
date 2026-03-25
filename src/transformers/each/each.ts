import { Stream } from "../../streams/index.ts";
import { Map } from "../map/map.ts";

const NAME = "each";

export class Each<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = each.Name,
> extends Map<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME> {
  constructor(
    options: Stream.TransformOptions<INPUT_STREAM, NAME> & {
      callback: each.Callback<CLEAN_VALUE, ERROR>;
    },
  ) {
    super({
      inputStream: options.inputStream,
      name: options.name ?? (NAME as NAME),
      token: options.token,
      mapper: async (value) => {
        const maybePromise = options.callback(value);
        const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

        return result ?? value;
      },
    });

    const self = this;
  }
}
export function each<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = each.Name,
>(
  callback: each.Callback<CLEAN_VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Each<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, ERROR, NAME>> {
  return (options) => new Each({ ...options, callback });
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<CLEAN_VALUE, ERROR> = (
    value: CLEAN_VALUE,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}
