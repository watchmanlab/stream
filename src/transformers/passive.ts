import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Subscribes to the upstream without requesting values via `next()`.
 * Values are only received when the upstream pushes them independently.
 * Useful for observing a shared stream without driving it.
 *
 * @example
 * source.pipe(passive()).pipe(listen(console.log));
 */
export class Passive<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(readonly $input: INPUT) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const input$ = this.$input.consume(handler, options);
    input$.setOption("next", options?.next);
    return input$;
  }
}

/**
 * Get a `Consumer` from the upstream without requesting values via `next()`.
 * Values are only received when the upstream pushes them independently.
 * Useful for observing a shared stream without driving it.
 *
 * @example
 * source.pipe(passive()).pipe(listen(console.log));
 */
export function passive<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Passive($input);
}
