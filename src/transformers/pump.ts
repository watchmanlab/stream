import { Producer } from "../core/producer";
import { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function pump<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$pump",
>(options?: Omit<Producer.Options<VALUE, NAME>, "source">): Transform<INPUT, NAME, Producer<VALUE, NAME>> {
  return (input) => {
    const { name, terminate, ...rest } = options ?? {};

    const output = new Producer({
      ...rest,
      name: name ?? ("$pump" as NAME),
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });

    const inputConsumer = input
      .consume((self, value) => {
        output.push(value);
        self.next();
      })
      .next();

    return output;
  };
}
