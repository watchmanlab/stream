import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Flat$<
  INPUT extends Consumable.AnyConsumable,
  DEPTH extends number = 1,
  VALUE extends ValueOfConsumable<INPUT, DEPTH> = ValueOfConsumable<INPUT, DEPTH>,
> extends Source<VALUE> {
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

    const output$ = new Consumer(handler, {
      ...rest,
      next(consumer) {
        value$.next();
        next?.(consumer);
      },
      terminate(consumer, reason) {
        terminate?.(consumer, reason);
        input$.terminate(reason);
        value$.terminate(reason);
      },
    });

    const input$ = this.$input.consume(
      (_, consumable) => {
        if (Consumable.isConsumable<VALUE>(consumable)) {
          value$ = consumable.consume(
            (_, value) => {
              output$.push(value);
            },
            {
              terminate: () => {
                value$ = input$;
                input$.next();
              },
            },
          );
          value$.next();
        } else {
          output$.push(consumable);
        }
      },
      { terminate: (_, reason) => output$.terminate(reason) },
    );
    let value$: Consumer<any> = input$;

    return output$;
  }
}

export function flat$<INPUT extends Consumable.AnyConsumable, DEPTH extends number = 1>(depth = 1 as DEPTH) {
  return ($input: INPUT) => new Flat$($input, depth);
}
