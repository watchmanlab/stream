import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason } from "../core/types";

export class TapTerminate<INPUT extends Consumable.AnyConsumable> extends Source<TerminateReason> {
  constructor(
    private $input: INPUT,
    private fn: (reason: TerminateReason) => void,
  ) {
    super();
  }

  override consume(
    handler: Consumer.Handler<TerminateReason>,
    options?: Consumer.Options<TerminateReason> | undefined,
  ): Consumer<TerminateReason> {
    const { terminate, ...rest } = options ?? {};
    const { fn } = this;
    return this.$input.consume(handler, {
      ...rest,
      terminate(c, r) {
        fn(r);
        terminate?.(c, r);
      },
    });
  }
}

export function tapTerminate<INPUT extends Consumable.AnyConsumable>(fn: (reason: TerminateReason) => void) {
  return ($input: INPUT) => new TapTerminate($input, fn);
}
