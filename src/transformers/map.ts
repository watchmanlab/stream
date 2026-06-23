import { stream } from "../core/stream";
import { transformer, Transformer } from "../core/transformer";

export class Map<
  INPUT extends stream.AnyStream,
  VALUE extends stream.ExtractValue<INPUT> = stream.ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
> extends Transformer<INPUT, MAPPED, NAME> {
  constructor(input: INPUT, mapper: map.Mapper<VALUE, MAPPED>, options?: map.Options<MAPPED, NAME>) {
    super(input, {
      ...options,
      name: options?.name ?? (map.NAME as NAME),
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
  INPUT extends stream.AnyStream,
  VALUE extends stream.ExtractValue<INPUT> = stream.ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<VALUE, MAPPED>,
  options?: map.Options<MAPPED, NAME>,
): stream.Transform<INPUT, NAME, Map<INPUT, VALUE, MAPPED, NAME>> {
  return (input, name) => new Map(input, mapper, { ...options, name });
}
export namespace map {
  export const NAME = "map";
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
  export type Options<MAPPED, NAME extends string> = transformer.Options<MAPPED, NAME>;
}
