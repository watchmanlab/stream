import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Pipe<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    pipeline: ($input: INPUT) => void,
  ) {
    super();
    pipeline($input);
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    return this.$input.consume(handler, options);
  }
}

export function pipe<INPUT extends Consumable.AnyConsumable>(pipeline: ($input: INPUT) => void) {
  return ($input: INPUT) => new Pipe($input, pipeline);
}
