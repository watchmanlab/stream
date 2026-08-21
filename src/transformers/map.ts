import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Transformer } from "../core/transformer";
import { ExtractValue } from "../core/types";

export class Map<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  MAPPED = VALUE,
>
  extends Source<MAPPED>
  implements Transformer<INPUT, MAPPED>
{
  constructor(
    readonly $input: INPUT,
    private mapper: Map.Mapper<VALUE, MAPPED>,
  ) {
    super();
  }
  override consume(options?: Consumer.Options<MAPPED>): Consumer<MAPPED> {
    return this.$input.consume(new InputConsumerOptions(this.mapper, options));
  }
}

class InputConsumerOptions<VALUE, MAPPED> extends Consumer.DefaultOptions<MAPPED> {
  constructor(
    private mapper: Map.Mapper<VALUE, MAPPED>,
    options?: Consumer.Options<MAPPED>,
  ) {
    super(options);
  }
  override handler(consumer: Consumer<MAPPED>, value: any): void {
    this.options?.handler?.(consumer, this.mapper(value));
  }
}

export function map<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  MAPPED = VALUE,
>(mapper: Map.Mapper<VALUE, MAPPED>) {
  return ($input: INPUT) => new Map($input, mapper);
}

export namespace Map {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
