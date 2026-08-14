import { Producer } from "../core/producer";
import { ExtractValue, NonEmptyString, Transform } from "../core/types";

export function mapBatch<
  INPUT extends Producer<any[], any>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
  MAPPED = VALUE,
  NAME extends NonEmptyString = "$mapBatch",
>(
  mapper: MapBatch.Mapper<VALUE, MAPPED>,
  options?: Producer.Options<MAPPED[], NAME>,
): Transform<INPUT, NAME, Producer<MAPPED[], NAME>> {
  return (input) => {
    const { name, ...rest } = options ?? {};

    const results: MAPPED[] = [];

    const output = new Producer({
      ...rest,
      name: name ?? ("$mapBatch" as NAME),
      source: {
        consume() {
          return input.consume((_, values) => {
            results.length = 0;
            for (let i = 0, len = values.length; i < len; i++) {
              results.push(mapper(values[i]));
            }
            output.push(results);
            results.length = 0;
          });
        },
      },
    });

    return output;
  };
}
export namespace MapBatch {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
