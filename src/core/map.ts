import { Consumer } from "./consumer";
import { Stream } from "./stream";
import { Transformer } from "./transformer";

export class Map<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
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
          const inputConsumer = input.listen((self, value) => {
            outputConsumer.push(mapper(value));
          });
          const outputConsumer = new Consumer<MAPPED, any>({
            ...init,
            ready: (self) => {
              inputConsumer.next();
              init.ready?.(self);
            },
          });
          return outputConsumer;
        },
      },
    });
  }
}
export function map<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
>(mapper: map.Mapper<VALUE, MAPPED>): Stream.Transform<INPUT, NAME, Map<INPUT, VALUE, MAPPED, NAME>> {
  return (input, name) => new Map(name, input, mapper);
}
export namespace map {
  export const NAME = "map";
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
