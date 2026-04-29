import { Stream } from "../../stream";

const NAME = "effect";

class Effect<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  ERROR = unknown,
  NAME extends string = effect.Name,
> extends Stream<VALUE, ERROR, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, callback: effect.Callback<VALUE, ERROR>) {
    super(name, async function* () {
      for await (const value of inputStream) {
        queueMicrotask(async () => {
          const result = await callback(value);
          if (result instanceof Stream.Error) throw result;
        });
        yield value;
      }
    });
  }
}

export function effect<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  ERROR = unknown,
  NAME extends string = effect.Name,
>(
  callback: effect.Callback<VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Stream.Transformer<Effect<INPUT_STREAM, VALUE, ERROR, NAME>, INPUT_STREAM>> {
  return (inputStream, name) => Stream.transformer(new Effect(name, inputStream, callback), inputStream);
}

export namespace effect {
  export type Name = typeof NAME;
  export type Callback<VALUE, ERROR> = (
    value: VALUE,
  ) => void | Stream.Error<ERROR> | Promise<void | Stream.Error<ERROR>>;
}

const stream = new Stream([1, 2, 3]).pipe(
  effect((v) => {
    if (v == 2) return;
  }),
);
