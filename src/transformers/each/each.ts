import { Stream } from "../../streams/index.ts";
import { Map } from "../map/map.ts";

const NAME = "each";

export class Each<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = each.Name,
> extends Map<
  INPUT_STREAM,
  INPUT_NAME,
  [SELF] extends [never] ? Each<INPUT_STREAM, INPUT_NAME, never, CLEAN_VALUE, ERROR, NAME> : SELF,
  CLEAN_VALUE,
  CLEAN_VALUE,
  ERROR,
  NAME
> {
  constructor(
    options: Stream.TransformOptions<INPUT_STREAM, NAME> & {
      callback: each.Callback<
        CLEAN_VALUE,
        ERROR,
        Stream.Transformer<Each<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
      >;
    },
  ) {
    super({
      inputStream: options.inputStream,
      name: options.name ?? (NAME as NAME),
      token: options.token,
      mapper: async (value) => {
        const maybePromise = options.callback(value, self as never);
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
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = each.Name,
>(
  callback: each.Callback<
    CLEAN_VALUE,
    ERROR,
    Stream.Transformer<Each<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
  >,
): Stream.Transform<INPUT_STREAM, NAME, Each<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, ERROR, NAME>> {
  return (options) => new Each({ ...options, callback });
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<CLEAN_VALUE, ERROR, SELF extends Stream<any, any>> = (
    value: CLEAN_VALUE,
    self: SELF,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}
