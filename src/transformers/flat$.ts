import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, Consumable, ExtractValue, Transformer } from "../core/types";

export class Flat$<
  INPUT extends Consumable<AnyConsumable>,
  DEPTH extends number = 1,
  VALUE extends ExtractValue<INPUT, DEPTH> = ExtractValue<INPUT, DEPTH>,
>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    depth = 1 as DEPTH,
  ) {
    super();

    let flat$: Flat$<any, any, any> = this;

    while (depth-- > 1) {
      flat$ = new Flat$(flat$);
    }

    return flat$;
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const outputConsumer = new Consumer(handler, {
      ...rest,
      next(consumer) {
        valueConsumer.next();
        next?.(consumer);
      },
      terminate(consumer, reason) {
        inputConsumer.terminate(reason);
        valueConsumer.terminate(reason);
        terminate?.(consumer, reason);
      },
    });

    const inputConsumer = this.$input.consume(
      (_, consumable) => {
        if (typeof consumable === "object" && "consume" in consumable) {
          valueConsumer = consumable.consume(
            (_, value) => {
              outputConsumer.push(value);
            },
            {
              terminate: () => {
                valueConsumer = inputConsumer;
                inputConsumer.next();
              },
            },
          );
          valueConsumer.next();
        } else {
          outputConsumer.push(consumable);
        }
      },
      { terminate: (_, reason) => outputConsumer.terminate(reason) },
    );
    let valueConsumer: Consumer<any> = inputConsumer;

    return outputConsumer;
  }
}

export function flat$<INPUT extends AnyConsumable, DEPTH extends number = 1>(depth = 1 as DEPTH) {
  return ($input: INPUT) => new Flat$($input, depth);
}
