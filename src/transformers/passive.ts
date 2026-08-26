import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class Passive<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
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

export function passive<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Passive($input);
}
