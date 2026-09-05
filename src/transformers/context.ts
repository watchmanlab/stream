import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Wraps each value with a shared mutable context object: `{ value, context }`.
 * The same context reference is shared across all values in the stream.
 *
 * @example
 * of(1,2,3).pipe(context({ count: 0 })).pipe(tap(v => v.context.count++)).pipe(listen());
 */
export class Context<
  INPUT extends Consumable.AnyConsumable,
  CTX,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<Context.Value<VALUE, CTX>> {
  constructor(
    private $input: INPUT,
    private context: CTX,
  ) {
    super();
  }

  override consume(
    handler: Consumer.Handler<Context.Value<VALUE, CTX>>,
    options?: Consumer.Options<Context.Value<VALUE, CTX>> | undefined,
  ): Consumer<Context.Value<VALUE, CTX>> {
    const { context } = this;
    return this.$input.consume((c, value) => handler(c, { value, context }), options);
  }
}

export namespace Context {
  export type Value<VALUE, CTX> = { value: VALUE; context: CTX };
}

/**
 * Wraps each value with a shared mutable context object: `{ value, context }`.
 * The same context reference is shared across all values in the stream.
 *
 * @param context The shared context object attached to every value.
 *
 * @example
 * of(1,2,3).pipe(context({ count: 0 })).pipe(tap(v => v.context.count++)).pipe(listen());
 */
export function context<INPUT extends Consumable.AnyConsumable, CTX>(context: CTX) {
  return ($input: INPUT) => new Context($input, context);
}
