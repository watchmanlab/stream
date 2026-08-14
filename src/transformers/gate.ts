import { Consumer } from "../core/consumer";
import { Producer } from "../core/producer";
import { AnyProducer, ExtractValue, NonEmptyString } from "../core/types";

export function gate<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$gate",
>(control: Producer<boolean, any>, options?: Omit<Producer.Options<VALUE, NAME>, "source">) {
  return (input: INPUT) => {
    const { name, next, terminate, ...rest } = options ?? {};

    let inputConsumer: Consumer<VALUE> | undefined;

    const output = new Producer<VALUE, NAME>({
      ...rest,
      name: name ?? ("$gate" as NAME),
      next(self, consumer) {
        inputConsumer?.next();
        next?.(self, consumer);
      },
      terminate(self, reason) {
        inputConsumer?.terminate(reason);
        controlConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });

    const controlConsumer = control
      .consume((self, value) => {
        if (value) {
          inputConsumer = input.consume((self, value) => {
            output.push(value);
          });
        } else {
          inputConsumer?.terminate("complete");
          inputConsumer = undefined;
        }
        self.next();
      })
      .next();

    return output;
  };
}
