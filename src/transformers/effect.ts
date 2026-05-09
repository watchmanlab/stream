import { Source, Stream, Transformer } from "../core";

const NAME = "effect";
export class Effect<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  ERROR = unknown,
  NAME extends string = effect.Name,
> extends Transformer<INPUT_STREAM, VALUE, ERROR, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, callback: effect.Callback<VALUE, ERROR>) {
    super(name, inputStream, async function* () {
      for await (const batch of inputStream) {
        (async () => {
          for (let i = 0, length = batch.length; i < length; i++) {
            try {
              let result = callback(batch[i]);

              if (result instanceof Promise) {
                result
                  .then((res) => {
                    if (res instanceof Source.Error) self.source?.throw(res.data);
                  })
                  .catch((error) => {
                    self.source?.throw(error);
                  });
              } else if (result instanceof Source.Error) {
                self.source?.throw(result.data);
              }
            } catch (error: any) {
              self.source?.throw(error);
            }
          }
        })();
        yield batch;
      }
    });
    const self = this;
  }
}

export function effect<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  ERROR = unknown,
  NAME extends string = effect.Name,
>(
  callback: effect.Callback<VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Effect<INPUT_STREAM, VALUE, ERROR, NAME>> {
  return (inputStream, name) => new Effect(name, inputStream, callback);
}

export namespace effect {
  export type Name = typeof NAME;
  export type Callback<VALUE, ERROR> = (
    value: VALUE,
  ) => void | Source.Error<ERROR> | Promise<void | Source.Error<ERROR>>;
}
