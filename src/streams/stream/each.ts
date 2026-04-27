import { Stream } from "./stream";

const NAME = "each";

class Each<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  ERROR = unknown,
  NAME extends string = each.Name,
> extends Stream<VALUE, ERROR, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, callback: each.Callback<VALUE, ERROR>) {
    super(name, async function* () {
      for await (const value of inputStream) {
        const maybePromise = callback(value);
        const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;
        yield result?.value ?? value;
      }
    });
  }
}

export function each<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  ERROR = unknown,
  NAME extends string = each.Name,
>(
  callback: each.Callback<VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Stream.Transformer<Each<INPUT_STREAM, VALUE, ERROR, NAME>, INPUT_STREAM>> {
  return (inputStream, name) => Stream.transformer(new Each(name, inputStream, callback), inputStream);
}

export namespace each {
  export type Name = typeof NAME;
  export type Callback<VALUE, ERROR> = (
    value: VALUE,
  ) => void | Stream.Error<ERROR> | Promise<void | Stream.Error<ERROR>>;
}
