import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

import { AnyConsumable, AnyConsumer, ExtractValue, Transformer } from "../core/types";

export class Merge<
  INPUT extends AnyConsumable,
  OTHERS extends [other: AnyConsumable, ...others: AnyConsumable[]],
  VALUE extends ExtractValue<INPUT> | ExtractValue<OTHERS[number]> = ExtractValue<INPUT> | ExtractValue<OTHERS[number]>,
>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  private _consumables: OTHERS;
  constructor(
    readonly $input: INPUT,
    ...others: OTHERS
  ) {
    super();
    this._consumables = others;
    this._consumables.reverse();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};
    const { _consumables } = this;

    type Entry = { consumer: AnyConsumer; pending: boolean };

    const outputConsumer = new Consumer(handler, {
      ...rest,
      next(consumer) {
        for (let i = 0; i < _consumables.length; i++) {
          const entry = _consumables[i] as unknown as Entry;
          if (!entry.pending) {
            entry.pending = true;
            entry.consumer.next();
          }
        }

        next?.(consumer);
      },
      terminate(consumer, reason) {
        for (let i = 0; i < _consumables.length; i++) {
          const entry = _consumables[i] as unknown as Entry;
          entry.consumer.terminate(reason);
        }
        _consumables.length = 0;
        terminate?.(consumer, reason);
      },
    });

    for (let i = 0; i < _consumables.length; i++) {
      const entry: Entry = {
        consumer: this._consumables[i].consume((_, value) => {
          entry.pending = false;
          outputConsumer.push(value);
        }),
        pending: false,
      };

      _consumables[i] = entry as any;
    }

    const inputEntry: Entry = {
      consumer: this.$input.consume(
        (_, value) => {
          inputEntry.pending = false;
          outputConsumer.push(value);
        },
        {
          terminate(_, reason) {
            outputConsumer.terminate(reason);
          },
        },
      ),
      pending: false,
    };
    _consumables.push(inputEntry as any);

    return outputConsumer;
  }
}

export function merge<INPUT extends AnyConsumable, OTHERS extends [other: AnyConsumable, ...others: AnyConsumable[]]>(
  ...others: OTHERS
) {
  return ($input: INPUT) => new Merge($input, ...others);
}
