import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason } from "../core/types";

export class Terminate<INPUT extends Consumable.AnyConsumable> extends Source<TerminateReason> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<TerminateReason>,
    options?: Consumer.Options<TerminateReason> | undefined,
  ): Consumer<TerminateReason> {
    const { terminate, ...rest } = options ?? {};

    return this.$input.consume((c) => c.next(), {
      ...rest,
      terminate(c, r) {
        handler(c, r);
        terminate?.(c, r);
      },
    });
  }
}

export function terminate<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Terminate($input);
}
