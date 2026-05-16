import { Stream, Transformer } from "../core/index.ts";

const NAME = "each";
export class Each<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = each.Name,
> extends Transformer<INPUT_STREAM, VALUE, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, callback: each.Callback<VALUE>) {
    super(
      name,
      inputStream,
      inputStream.channels.get({
        next: async (batch) => {
          for (let i = 0, length = batch.length; i < length; i++) {
            try {
              const value = batch[i];
              const result = callback(value);
              if (result instanceof Promise) await result;
              this.push(value);
              this.source?.ready();
            } catch (error) {
              this.source?.throw(error);
            }
          }
        },
        return: () => this.source?.return(),
      }),
    );
    const self = this;
  }
}

export function each<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = each.Name,
>(callback: each.Callback<VALUE>): Stream.Transform<INPUT_STREAM, NAME, Each<INPUT_STREAM, VALUE, NAME>> {
  return (inputStream, name) => new Each(name, inputStream, callback);
}

export namespace each {
  export type Name = typeof NAME;
  export type Callback<VALUE> = (value: VALUE) => void | Promise<void>;
}
