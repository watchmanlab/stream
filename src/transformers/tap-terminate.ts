import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason, ValueOfConsumable } from "../core/types";

/**
 * Runs a side-effect callback when the stream terminates, passing the termination reason.
 * Values pass through unchanged.
 *
 * @example
 * of(1, 2, 3).pipe(tapTerminate(reason => console.log('done:', reason))).pipe(listen());
 */
export class TapTerminate<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private fn: (reason: TerminateReason) => void,
  ) {
    super();
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
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
/**
 * Runs a side-effect callback when the stream terminates, passing the termination reason.
 * Values pass through unchanged.
 *
 * @param fn Callback invoked with the termination reason.
 *
 * @example
 * of(1, 2, 3).pipe(tapTerminate(reason => console.log('done:', reason))).pipe(listen());
 */
export function tapTerminate<INPUT extends Consumable.AnyConsumable>(fn: (reason: TerminateReason) => void) {
  return ($input: INPUT) => new TapTerminate($input, fn);
}
