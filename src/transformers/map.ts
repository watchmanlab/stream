import { Stream, Transformer } from "../core/index.ts";

const NAME = "map";
class Map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
> extends Transformer<INPUT_STREAM, MAPPED, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<VALUE, MAPPED>) {
    super(name, inputStream, () => {
      const channel = inputStream.channels.get();

      return {
        next: () => {
          channel.next(
            (batch) => {
              for (let i = 0, length = batch.length; i < length; i++) {
                try {
                  let value = mapper(batch[i]);
                  if (value instanceof Promise) {
                    value.then((value) => this.push(value)).catch((error) => this.source?.throw(error));
                  } else {
                    this.push(value);
                  }
                } catch (error) {
                  this.source?.throw(error);
                } finally {
                  this.source?.ready();
                }
              }
            },
            () => {
              this.source?.return();
            },
          );
        },
        return: () => {
          channel.return();
        },
      };
    });
  }
}
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
>(mapper: map.Mapper<VALUE, MAPPED>): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, VALUE, MAPPED, NAME>> {
  return (inputStream, name) => {
    return new Map(name, inputStream, mapper);
  };
}

export namespace map {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED | Promise<MAPPED>;
}
