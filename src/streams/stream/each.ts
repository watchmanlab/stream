import { Stream } from "./stream9";

const NAME = "each";

class Each<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = each.Name,
> extends Stream<Stream.ExtractValue<INPUT_STREAM>, NAME> {
  constructor(name: NAME, inputStream: INPUT_STREAM, callback: each.Callback<CLEAN_VALUE, ERROR>) {
    super(name);
  }
}

export namespace each {
  export type Name = typeof NAME;
  export type Callback<CLEAN_VALUE, ERROR> = (
    value: CLEAN_VALUE,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}
