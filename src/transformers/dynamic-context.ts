import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class DynamicContext<
  INPUT extends Consumable.AnyConsumable,
  CTX,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<DynamicContext.Wrapper<VALUE, CTX>> {
  constructor(
    private $input: INPUT,
    private contextFn: () => CTX,
  ) {
    super();
  }

  override consume(
    handler: Consumer.Handler<DynamicContext.Wrapper<VALUE, CTX>>,
    options?: Consumer.Options<DynamicContext.Wrapper<VALUE, CTX>> | undefined,
  ): Consumer<DynamicContext.Wrapper<VALUE, CTX>> {
    const { contextFn } = this;
    const context = contextFn();
    return this.$input.consume((c, value) => handler(c, { value, context }), options);
  }
}

export namespace DynamicContext {
  export type Wrapper<VALUE, CTX> = { value: VALUE; context: CTX };
}

export function context<INPUT extends Consumable.AnyConsumable, CTX>(contextFn: () => CTX) {
  return ($input: INPUT) => new DynamicContext($input, contextFn);
}
