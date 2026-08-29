import { EMPTY_FUNCTION } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";
import { passive } from "./passive";

export class DependOn<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [other: Consumable.AnyConsumable, ...others: Consumable.AnyConsumable[]],
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private others: OTHERS,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};

    const output$ = this.$input.consume(handler, {
      ...rest,
      terminate(consumer, reason) {
        others$.forEach((consumer) => consumer.terminate(reason));
        terminate?.(consumer, reason);
      },
    });
    const others$ = this.others.map((other) =>
      Source.from(other)
        .pipe(passive())
        .consume((consumer) => consumer.next(), {
          terminate: (_, reason) => output$.terminate(reason),
        })
        .next(),
    );

    return output$;
  }
}

export function dependOn<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [other: Consumable.AnyConsumable, ...others: Consumable.AnyConsumable[]],
>(...others: OTHERS) {
  return ($input: INPUT) => new DependOn($input, others);
}
