import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Result } from "../core/types";

export class Min<INPUT extends Consumable<number>> extends Source<Result<number, "not-found">> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<Result<number, "not-found">>,
    options?: Consumer.Options<Result<number, "not-found">> | undefined,
  ): Consumer<Result<number, "not-found">> {
    const { next, terminate, ...rest } = options ?? {};

    let min = null as number | null;

    const input$ = this.$input.consume(
      (c, v) => {
        if (!min) {
          min = v;
        } else {
          min = min < v ? min : v;
        }
        c.next();
      },
      {
        terminate(c, r) {
          min ? output$.push({ ok: true, value: min }) : output$.push({ ok: false, error: "not-found" });
          output$.terminate(r);
        },
      },
    );

    const output$ = new Consumer(handler, {
      ...rest,
      next(c) {
        input$.next();
        next?.(c);
      },
      terminate(c, r) {
        input$.terminate(r);
        terminate?.(c, r);
      },
    });

    return output$;
  }
}

export function min<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Min($input);
}
