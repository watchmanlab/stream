import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Stream } from "../core/stream";
import { ValueOfConsumable } from "../core/types";

/**
 * Eagerly drains the upstream and trigger the consumption chain for a dormant pipeline.
 *
 * @example
 *  of(1, 2, 3).pipe(map( v => v * 2)).pipe(tap(console.log)).pipe(pump());
 */
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

/**
 * Eagerly drains the upstream and trigger the consumption chain for a dormant pipeline.
 *
 * @example
 *  stream.pipe(map( v => v * 2)).pipe(tap(console.log)).pipe(pump());
 */
export function pump<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Pump($input);
}
