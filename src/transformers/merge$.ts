import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class Merge$<
  INPUT extends Consumable.AnyConsumable,
  DEPTH extends number = 1,
  VALUE extends ExtractValue<INPUT, DEPTH> = ExtractValue<INPUT, DEPTH>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private concurrent = Infinity,
    depth = 1 as DEPTH,
  ) {
    super();

    let merge$: Merge$<any, any, any> = this;

    while (depth-- > 1) {
      merge$ = new Merge$(merge$, concurrent);
    }

    return merge$;
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};
    const { $input, concurrent } = this;

    const consumers$ = new Set<Consumer<any>>();

    const output$ = new Consumer(handler, {
      ...rest,
      next(consumer) {
        next?.(consumer);
        if (consumers$.size < concurrent) {
          input$.next();
        } else {
        }
      },
      terminate(consumer, reason) {
        terminate?.(consumer, reason);
        input$.terminate(reason);
      },
    });

    const input$ = $input.consume(
      (_, consumable) => {
        if (Consumable.isConsumable<VALUE>(consumable)) {
          consumers$.add(
            consumable.consume((c, v) => output$.push(v), {
              terminate(c, r) {
                consumers$.delete(c);
                input$.next();
              },
            }),
          );
        } else {
          output$.push(consumable);
        }
      },
      { terminate: (_, reason) => output$.terminate(reason) },
    );

    return output$;
  }
}

export function merge$<INPUT extends Consumable.AnyConsumable, DEPTH extends number = 1>(
  concurrent = Infinity,
  depth = 1 as DEPTH,
) {
  return ($input: INPUT) => new Merge$($input, concurrent, depth);
}
