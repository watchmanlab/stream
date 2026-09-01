import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Stream } from "../core/stream";
import { ValueOfConsumable } from "../core/types";

export class Pump<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  private $stream = new Stream<VALUE>();
  constructor($input: INPUT) {
    super();

    $input
      .consume((c, v) => {
        this.$stream.push(v);
        c.next();
      })
      .next();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    return this.$stream.consume(handler, options);
  }
}

export function pump<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Pump($input);
}
