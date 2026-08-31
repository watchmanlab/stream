import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Result } from "../core/types";

export class Max<INPUT extends Consumable<number>> extends Source<Result<number, "not-found">> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<Result<number, "not-found">>,
    options?: Consumer.Options<Result<number, "not-found">> | undefined,
  ): Consumer<Result<number, "not-found">> {
    const { next, terminate, ...rest } = options ?? {};

    let max = null as number | null;

    const input$ = this.$input.consume(
      (c, v) => {
        if (!max) {
          max = v;
        } else {
          max = max < v ? v : max;
        }
        c.next();
      },
      {
        terminate(c, r) {
          max ? output$.push({ ok: true, value: max }) : output$.push({ ok: false, error: "not-found" });
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

export function max<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Max($input);
}
