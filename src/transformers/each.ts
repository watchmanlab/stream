import { Source, Stream, Transformer } from "../core";

const NAME = "each";
export class Each<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  ERROR = unknown,
  NAME extends string = each.Name,
> extends Transformer<INPUT_STREAM, VALUE, ERROR, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, callback: each.Callback<VALUE, ERROR>) {
    super(name, inputStream, async function* () {
      for await (const batch of inputStream) {
        for (let i = 0, length = batch.length; i < length; i++) {
          try {
            let result = callback(batch[i]);

            result = result instanceof Promise ? await result : result;

            if (result instanceof Source.Error) self.source?.throw(result.data);
          } catch (error: any) {
            self.source?.throw(error);
          }
        }

        yield batch;
      }
    });
    const self = this;
  }
}

export function each<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  ERROR = unknown,
  NAME extends string = each.Name,
>(callback: each.Callback<VALUE, ERROR>): Stream.Transform<INPUT_STREAM, NAME, Each<INPUT_STREAM, VALUE, ERROR, NAME>> {
  return (inputStream, name) => new Each(name, inputStream, callback);
}

export namespace each {
  export type Name = typeof NAME;
  export type Callback<VALUE, ERROR> = (
    value: VALUE,
  ) => void | Source.Error<ERROR> | Promise<void | Source.Error<ERROR>>;
}
