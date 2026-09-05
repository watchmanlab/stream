import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Logs each value to the console with an optional label, then passes it through unchanged.
 *
 * @example
 * of(1, 2, 3).pipe(print('val:')).pipe(listen());
 */
export class Print<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private label?: string,
  ) {
    super();
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    return this.$input.consume(
      (c, v) => (this.label ? console.log(this.label, v) : console.log(v), handler(c, v)),
      options,
    );
  }
}

/**
 * Creates a `print` transformer.
 * @param label Optional prefix label for console output.
 */
export function print<INPUT extends Consumable.AnyConsumable>(label?: string) {
  return ($input: INPUT) => new Print($input, label);
}
