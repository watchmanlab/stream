import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Delay<
  INPUT extends Consumable.AnyConsumable,
  MS extends number,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private ms: MS,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume((consumer, value) => setTimeout(() => handler(consumer, value), this.ms), options);
  }
}

export function delay<INPUT extends Consumable.AnyConsumable, MS extends number>(ms: MS) {
  return ($input: INPUT) => new Delay($input, ms);
}
