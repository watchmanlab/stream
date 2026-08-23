import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class SkipUntil<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private $notifier: Consumable.AnyConsumable,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};

    let skip = true;
    const notifier$ = this.$notifier
      .consume((consumer) => {
        skip = false;
        consumer.terminate("complete");
      })
      .next();

    return this.$input.consume(
      (consumer, value) => {
        if (skip) {
          consumer.next();
        } else {
          consumer["_handler"] = handler;
          handler(consumer, value);
        }
      },
      {
        ...rest,
        terminate(consumer, reason) {
          notifier$.terminate(reason);
          terminate?.(consumer, reason);
        },
      },
    );
  }
}

export function skipUntil<INPUT extends Consumable.AnyConsumable>($notifier: Consumable.AnyConsumable) {
  return ($input: INPUT) => new SkipUntil($input, $notifier);
}
