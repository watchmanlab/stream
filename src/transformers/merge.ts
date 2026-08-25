import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

import { ExtractValue } from "../core/types";

export class Merge<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [other: Consumable.AnyConsumable, ...others: Consumable.AnyConsumable[]],
  VALUE extends ExtractValue<INPUT> | ExtractValue<OTHERS[number]> = ExtractValue<INPUT> | ExtractValue<OTHERS[number]>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private $others: OTHERS,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};
    let consumables = this.$others;
    consumables.reverse();

    const others$ = this.$others.map((other) =>
      other.consume((c, v) => {
        output$.push(v);
      }),
    );

    type Entry = { consumer: Consumer.AnyConsumer; pending: boolean };

    const output$ = new Consumer(handler, {
      ...rest,
      next(consumer) {
        for (let i = 0; i < consumables.length; i++) {
          const entry = consumables[i] as unknown as Entry;
          if (!entry.pending) {
            entry.pending = true;
            entry.consumer.next();
          }
        }

        next?.(consumer);
      },
      terminate(consumer, reason) {
        for (let i = 0; i < consumables.length; i++) {
          const entry = consumables[i] as unknown as Entry;
          entry.consumer.terminate(reason);
        }
        consumables.length = 0;
        terminate?.(consumer, reason);
      },
    });

    for (let i = 0; i < consumables.length; i++) {
      const entry: Entry = {
        consumer: consumables[i].consume((_, value) => {
          entry.pending = false;
          output$.push(value);
        }),
        pending: false,
      };

      consumables[i] = entry as any;
    }

    const inputEntry: Entry = {
      consumer: this.$input.consume(
        (_, value) => {
          inputEntry.pending = false;
          output$.push(value);
        },
        {
          terminate(_, reason) {
            output$.terminate(reason);
          },
        },
      ),
      pending: false,
    };
    consumables.push(inputEntry as any);

    return output$;
  }
}

export function merge<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [other: Consumable.AnyConsumable, ...others: Consumable.AnyConsumable[]],
>(...others: OTHERS) {
  return ($input: INPUT) => new Merge($input, others);
}
