import { stream } from "../core/stream";
import { Transformer } from "../core/transformer";

export class Map<
  INPUT extends stream.AnyStream,
  VALUE extends stream.ExtractValue<INPUT> = stream.ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
> extends Transformer<INPUT, MAPPED, NAME> {
  constructor(
    name = map.NAME as NAME,
    input: INPUT,
    public readonly mapper: map.Mapper<VALUE, MAPPED>,
  ) {
    super(name, input, {
      source: {
        listen: (init) => {
          return input.listen({
            ...init,
            handler: (self, value) => {
              init.handler(self, mapper(value));
            },
          });
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
>(mapper: map.Mapper<VALUE, MAPPED>): stream.Transform<INPUT, NAME, Map<INPUT, VALUE, MAPPED, NAME>> {
  return (input, name) => new Map(name, input, mapper);
}
export namespace map {
  export const NAME = "map";
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
