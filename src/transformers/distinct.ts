import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class Distinct<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  KEY = VALUE,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private keySelector?: (value: VALUE) => KEY,
    private $flushes?: Consumable.AnyConsumable,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};
    const { keySelector } = this;
    const valuesOrKeys = new Set<VALUE | KEY>();

    const input$ = this.$input.consume(
      keySelector
        ? (c, v) => {
            const key = keySelector(v);
            if (valuesOrKeys.has(key)) {
              c.next();
            } else {
              valuesOrKeys.add(key);
              handler(c, v);
            }
          }
        : (c, v) => {
            if (valuesOrKeys.has(v)) {
              c.next();
            } else {
              valuesOrKeys.add(v);
              handler(c, v);
            }
          },
      {
        ...rest,
        terminate(c, r) {
          valuesOrKeys.clear();
          flushes$?.terminate(r);
          terminate?.(c, r);
        },
      },
    );

    const flushes$ = this.$flushes
      ?.consume(
        (c) => {
          valuesOrKeys.clear();
          c.next();
        },
        {
          terminate(c, r) {
            input$.terminate(r);
          },
        },
      )
      .next();
    return input$;
  }
}

export function distinct<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  KEY = VALUE,
>(keySelector?: (value: VALUE) => KEY, $flushes?: Consumable.AnyConsumable) {
  return ($input: INPUT) => new Distinct($input, keySelector, $flushes);
}
