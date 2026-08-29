import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Gate<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private control: Consumable<boolean>,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    let input$: Consumer<VALUE> | undefined;

    const output$ = new Consumer<VALUE>(handler, {
      ...rest,
      next(consumer) {
        input$?.next();
        next?.(consumer);
      },
      terminate(consumer, reason) {
        input$?.terminate(reason);
        control$.terminate(reason);
        terminate?.(consumer, reason);
      },
    });

    const control$ = this.control
      .consume(
        (self, value) => {
          if (value) {
            input$ = this.$input.consume((_, value) => output$.push(value)).next();
          } else {
            input$?.terminate("complete");
            input$ = undefined;
          }
          self.next();
        },
        {
          terminate(consumer, reason) {
            output$.terminate(reason);
          },
        },
      )
      .next();

    return output$;
  }
}

export function gate<INPUT extends Consumable.AnyConsumable>(control: Consumable<boolean>) {
  return ($input: INPUT) => new Gate($input, control);
}
