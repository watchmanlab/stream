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
    return this.$input.consume(new ConsumerOptions(this.mapper, options));
  }
}

class ConsumerOptions extends Consumer.DefaultOptions<any> {
  constructor(
    private mapper: Map.Mapper<any, any>,
    options?: Consumer.Options<any>,
  ) {
    super(options);
  }
  override handler(consumer: Consumer<any>, value: any): void {
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
