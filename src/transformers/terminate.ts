import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason, ValueOfConsumable } from "../core/types";

export class Terminate<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private callback: (reason: TerminateReason) => void,
  ) {
    super();
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};
    const { callback } = this;
    return this.$input.consume(handler, {
      ...rest,
      terminate(c, r) {
        callback(r);
        terminate?.(c, r);
      },
    });
  }
}

export function terminate<INPUT extends Consumable.AnyConsumable>(callback: (reason: TerminateReason) => void) {
  return ($input: INPUT) => new Terminate($input, callback);
}
