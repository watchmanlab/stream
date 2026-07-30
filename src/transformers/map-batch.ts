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

    let results: any[];

    const inputConsumer = input.consume((_, values) => {
      for (let i = 0, len = values.length; i < len; i++) {
        results.push(mapper(values[i]));
      }
      this.push(results);
      results = [];
    });

    super(input, {
      ...rest,
      name: name ?? ("$mapBatch" as NAME),
      next(stream, consumer) {
        inputConsumer.next();
        next?.(stream, consumer);
      },
      terminate(stream, reason) {
        results = [];
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
