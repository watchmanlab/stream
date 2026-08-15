import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

import { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class Map<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>, MAPPED = VALUE>
  extends Source<MAPPED>
  implements Transformer<INPUT, MAPPED>
{
  constructor(
    readonly input: INPUT,
    private mapper: Map.Mapper<VALUE, MAPPED>,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<MAPPED>, options?: Consumer.Options<MAPPED>): Consumer<MAPPED> {
    return this.input.consume((consumer, value) => handler(consumer, this.mapper(value)), options);
  }
}

export function map<
  INPUT extends AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  MAPPED = VALUE,
>(mapper: Map.Mapper<VALUE, MAPPED>) {
  return (input: INPUT) => new Map(input, mapper);
}

export namespace Map {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
