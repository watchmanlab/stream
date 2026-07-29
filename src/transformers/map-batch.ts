import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class MapBatch<
  INPUT extends Stream<Array<any>, any>,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
  MAPPED = VALUE,
  NAME extends NonEmptyString = "$mapBatch",
> extends Transformer<INPUT, MAPPED[], NAME> {
  constructor(input: INPUT, mapper: MapBatch.Mapper<VALUE, MAPPED>, options?: Stream.Options<MAPPED[], NAME>) {
    const inputConsumer = input.consume((_, values) => {
      for (let i = 0; i < values.length; i++) {
        values[i] = mapper(values[i]);
      }
      this.push(values as MAPPED[]);
    });
    super(input, {
      ...options,
      name: options?.name ?? ("$mapBatch" as NAME),

      next(stream, consumer) {
        inputConsumer.next();
        options?.next?.(stream, consumer);
      },
      terminate(stream, reason) {
        inputConsumer.terminate(reason);
        options?.terminate?.(stream, reason);
      },
    });
  }
}
export function mapBatch<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
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
