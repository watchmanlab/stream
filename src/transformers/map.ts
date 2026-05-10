import { Stream, Transformer } from "../core/index.ts";

const NAME = "map";
class Map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = unknown,
  NAME extends string = map.Name,
> extends Transformer<INPUT_STREAM, MAPPED, ERROR, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<VALUE, MAPPED, ERROR>) {
    super(name, inputStream, async function* () {
      for await (const batch of inputStream) {
        const values: Stream.Batch<MAPPED> = [];

        for (let i = 0, length = batch.length; i < length; i++) {
          try {
            let result = mapper(batch[i]);
            result = result instanceof Promise ? await result : result;

            if (result instanceof Stream.Error) {
              self.throw(result.data);
              continue;
            }

            values.push(result);
          } catch (error: any) {
            self.throw(error);
          }
        }
        yield values;
      }
    });
    const self = this;
  }
}
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = unknown,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<VALUE, MAPPED, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>> {
  return (inputStream, name) => {
    return new Map(name, inputStream, mapper);
  };
}

export namespace map {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, ERROR> = (
    value: VALUE,
  ) => MAPPED | Stream.Error<ERROR> | Promise<MAPPED | Stream.Error<ERROR>>;
}
