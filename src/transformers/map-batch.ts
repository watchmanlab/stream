import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Consumable, ValueOfConsumable, Transformer } from "../core/types";

export class MapBatch<
  INPUT extends Consumable<any[]>,
  VALUE extends ValueOfConsumable<INPUT, 1> = ValueOfConsumable<INPUT, 1>,
  MAPPED = VALUE,
>
  extends Source<MAPPED[]>
  implements Transformer<INPUT, MAPPED[]>
{
  constructor(
    readonly $input: INPUT,
    private mapper: MapBatch.Mapper<VALUE, MAPPED>,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<MAPPED[]>, options?: Consumer.Options<MAPPED[]>): Consumer<MAPPED[]> {
    const results: MAPPED[] = [];
    return this.$input.consume(
      (consumer, values) => {
        results.length = 0;
        for (let i = 0, len = values.length; i < len; i++) {
          results.push(this.mapper(values[i]));
        }
        handler(consumer, results);
        results.length = 0;
      },
      { ...options },
    );
  }
}

export function mapBatch<
  INPUT extends Consumable<any[]>,
  VALUE extends ValueOfConsumable<INPUT, 1> = ValueOfConsumable<INPUT, 1>,
  MAPPED = VALUE,
>(mapper: MapBatch.Mapper<VALUE, MAPPED>) {
  return ($input: INPUT) => new MapBatch($input, mapper);
}

export namespace MapBatch {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
