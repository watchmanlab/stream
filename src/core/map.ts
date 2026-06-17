import { Consumer } from "./consumer";
import { Smoker } from "./smoker";
import { Transformer } from "./transformer";

export class Map<
  INPUT extends Smoker.AnySmoker,
  VALUE extends Smoker.ExtractValue<INPUT> = Smoker.ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
> extends Transformer<INPUT, MAPPED, NAME> {
  constructor(
    name = map.NAME as NAME,
    input: INPUT,
    public readonly mapper: map.Mapper<VALUE, MAPPED>,
  ) {
    const consumer = input.listen(
      (value) => {
        this.ready(mapper(value));
      },
      { isReady: false },
    );
    super(name, input, {
      source: consumer,
    });

    setTimeout(() => {
      console.log("map", [...consumer.get("queue")]);
    }, 100);
  }
}
export function map<
  INPUT extends Smoker.AnySmoker,
  VALUE extends Smoker.ExtractValue<INPUT> = Smoker.ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
>(mapper: map.Mapper<VALUE, MAPPED>): Smoker.Transform<INPUT, NAME, Map<INPUT, VALUE, MAPPED, NAME>> {
  return (input, name) => new Map(name, input, mapper);
}
export namespace map {
  export const NAME = "map";
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
