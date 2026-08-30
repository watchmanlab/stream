import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Pace<
  INPUT extends Consumable.AnyConsumable,
  MS extends number,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private ms: MS,
  ) {
    super();
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};
    const { ms, $input } = this;

    let timer: any = null;
    let nextAllowedExecutionTime = 0;

    return $input.consume(
      (consumer, value) => {
        const now = performance.now();

        if (now >= nextAllowedExecutionTime) {
          nextAllowedExecutionTime = now + ms;
          handler(consumer, value);
        } else {
          const delayRemainder = nextAllowedExecutionTime - now;
          nextAllowedExecutionTime += ms;

          timer = setTimeout(() => {
            handler(consumer, value);
          }, delayRemainder);
        }
      },
      {
        ...rest,

        terminate(consumer, reason) {
          terminate?.(consumer, reason);
          clearTimeout(timer);
        },
      },
    );
  }
}

export function pace<INPUT extends Consumable.AnyConsumable, MS extends number>(ms: MS) {
  return ($input: INPUT) => new Pace($input, ms);
}
