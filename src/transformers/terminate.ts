import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason } from "../core/types";

/**
 * Emits the termination reason when the stream completes or aborts, ignoring all values.
 *
 * @example
 * of(1, 2, 3).pipe(terminate()).pipe(listen(console.log)); // 'complete'
 */
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

/**
 * Emits the termination reason when the stream completes or aborts, ignoring all values.
 *
 * @example
 * of(1, 2, 3).pipe(terminate()).pipe(listen(console.log)); // 'complete'
 */
export function terminate<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Terminate($input);
}
