import { Stream, Transformer } from "../core/index.ts";

const NAME = "effect";
export class Effect<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = effect.Name,
> extends Transformer<INPUT_STREAM, VALUE, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, callback: effect.Callback<VALUE>) {
    super(
      name,
      inputStream,
      inputStream.channels.get({
        next: (batch) => {
          this.source?.ready();
          for (let i = 0, length = batch.length; i < length; i++) {
            try {
              callback(batch[i]);
            } catch (error) {
              //
            }
          }
          this.batch(batch);
        },
        return: () => this.source?.return(),
      }),
    );
  }
}

export function effect<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = effect.Name,
>(callback: effect.Callback<VALUE>): Stream.Transform<INPUT_STREAM, NAME, Effect<INPUT_STREAM, VALUE, NAME>> {
  return (inputStream, name) => new Effect(name, inputStream, callback);
}

export namespace effect {
  export type Name = typeof NAME;
  export type Callback<VALUE> = (value: VALUE) => void;
}
