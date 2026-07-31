import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class MapBatch<
  INPUT extends Stream<Array<any>, any>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
  MAPPED = VALUE,
  NAME extends NonEmptyString = "$mapBatch",
> extends Transformer<INPUT, MAPPED[], NAME> {
  constructor(input: INPUT, mapper: MapBatch.Mapper<VALUE, MAPPED>, options?: Stream.Options<MAPPED[], NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const results: MAPPED[] = [];

    const inputConsumer = input.consume((_, values) => {
      results.length = 0;
      for (let i = 0, len = values.length; i < len; i++) {
        results.push(mapper(values[i]));
      }
      this.push(results);
      results.length = 0;
    });

    super(input, {
      ...rest,
      name: name ?? ("$mapBatch" as NAME),
      next(stream, consumer) {
        inputConsumer.next();
        next?.(stream, consumer);
      },
      terminate(stream, reason) {
        results.length = 0;
        inputConsumer.terminate(reason);
        terminate?.(stream, reason);
      },
    });
  }
}
export function mapBatch<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
  MAPPED = VALUE,
  NAME extends NonEmptyString = "$mapBatch",
>(
  mapper: MapBatch.Mapper<VALUE, MAPPED>,
  options?: Stream.Options<MAPPED[], NAME>,
): Transform<INPUT, MapBatch<INPUT, VALUE, MAPPED, NAME>> {
  return (input) => new MapBatch(input, mapper, options);
}
export namespace MapBatch {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
