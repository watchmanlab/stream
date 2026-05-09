import { Source, Stream, Transformer } from "../core";

const NAME = "concurrent";
export class Concurrent<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  ERROR = unknown,
  NAME extends string = concurrent.Name,
> extends Transformer<INPUT_STREAM, VALUE, ERROR, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, callback: concurrent.Callback<VALUE, ERROR>) {
    super(name, inputStream, async function* () {
      for await (const batch of inputStream) {
        const promises: ReturnType<typeof callback>[] = [];
        for (let i = 0, length = batch.length; i < length; i++) {
          try {
            promises.push(callback(batch[i]));
          } catch (error: any) {
            self.source?.throw(error);
          }
        }

        const results = await Promise.allSettled(promises);
        promises.length = 0;
        for (let i = 0, length = results.length; i < length; i++) {
          const result = results[i];
          if (result.status === "rejected") {
            self.source?.throw(result.reason);
          } else if (result.value instanceof Source.Error) {
            self.source?.throw(result.value.data);
          }
        }

        yield batch;
      }
    });
    const self = this;
  }
}

export function concurrent<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  ERROR = unknown,
  NAME extends string = concurrent.Name,
>(
  callback: concurrent.Callback<VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Concurrent<INPUT_STREAM, VALUE, ERROR, NAME>> {
  return (inputStream, name) => new Concurrent(name, inputStream, callback);
}

export namespace concurrent {
  export type Name = typeof NAME;
  export type Callback<VALUE, ERROR> = (
    value: VALUE,
  ) => void | Source.Error<ERROR> | Promise<void | Source.Error<ERROR>>;
}
