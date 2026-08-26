import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class Context<
  INPUT extends Consumable.AnyConsumable,
  CTX,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<Context.Wrapper<VALUE, CTX>> {
  constructor(
    private $input: INPUT,
    private context: CTX,
  ) {
    super();
  }

  override consume(
    handler: Consumer.Handler<Context.Wrapper<VALUE, CTX>>,
    options?: Consumer.Options<Context.Wrapper<VALUE, CTX>> | undefined,
  ): Consumer<Context.Wrapper<VALUE, CTX>> {
    const { context } = this;
    return this.$input.consume((c, value) => handler(c, { value, context }), options);
  }
}

export namespace Context {
  export type Wrapper<VALUE, CTX> = { value: VALUE; context: CTX };
}

export function context<INPUT extends Consumable.AnyConsumable, CTX>(context: CTX) {
  return ($input: INPUT) => new Context($input, context);
}
