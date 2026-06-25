import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";

const NAME = "map";
export class Map<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends string = Map.Name,
> extends Transformer<INPUT, MAPPED, NAME, Map<INPUT, VALUE, MAPPED, NAME>> {
  constructor(input: INPUT, mapper: Map.Mapper<VALUE, MAPPED>, options?: Map.Options<INPUT, VALUE, MAPPED, NAME>) {
    super(input, {
      ...options,
      name: options?.name ?? (Map.NAME as NAME),
      source: {
        listen: (handler, options) => {
          return input.listen((self, value) => {
            handler(self, mapper(value));
          }, options);
        },
      },
    });
  }
}
export function map<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends string = Map.Name,
>(
  mapper: Map.Mapper<VALUE, MAPPED>,
  options?: Map.Options<INPUT, VALUE, MAPPED, NAME>,
): Stream.Transform<INPUT, NAME, Map<INPUT, VALUE, MAPPED, NAME>> {
  return (input, name) => new Map(input, mapper, { ...options, name });
}
export namespace Map {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
  export type Options<
    INPUT extends Stream.AnyStream,
    VALUE extends Stream.ExtractValue<INPUT>,
    MAPPED,
    NAME extends string,
  > = Transformer.Options<MAPPED, NAME, Map<INPUT, VALUE, MAPPED, NAME>>;
}
