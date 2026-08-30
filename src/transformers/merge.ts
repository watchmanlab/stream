import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

import { ValueOfConsumable } from "../core/types";

export class Merge<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [other: Consumable.AnyConsumable, ...others: Consumable.AnyConsumable[]],
  VALUE extends ValueOfConsumable<INPUT> | ValueOfConsumable<OTHERS[number]> =
    | ValueOfConsumable<INPUT>
    | ValueOfConsumable<OTHERS[number]>,
> extends Source<VALUE> {
  private $inputs: [INPUT, ...OTHERS];
  constructor(
    readonly $input: INPUT,
    $others: OTHERS,
  ) {
    super();

    this.$inputs = [$input, ...$others];
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const output$ = new Consumer(handler, {
      ...rest,
      next(consumer) {
        consumers.forEach((entry) => {
          if (!entry.pending) {
            entry.pending = true;
            entry.consumer.next();
          }
        });

        next?.(consumer);
      },
      terminate(consumer, reason) {
        consumers.forEach((entry) => entry.consumer.terminate(reason));
        consumers.length = 0;
        terminate?.(consumer, reason);
      },
    });

    const consumers = this.$inputs.map(($input, index) => {
      const entry = {
        consumer: $input.consume(
          (_, value) => {
            entry.pending = false;
            output$.push(value);
          },
          index === 0
            ? {
                terminate(_, reason) {
                  output$.terminate(reason);
                },
              }
            : undefined,
        ),
        pending: false,
      };
      return entry;
    });

    return output$;
  }
}

export function merge<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [other: Consumable.AnyConsumable, ...others: Consumable.AnyConsumable[]],
>(...others: OTHERS) {
  return ($input: INPUT) => new Merge($input, others);
}
