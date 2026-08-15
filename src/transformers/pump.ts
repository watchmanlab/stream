import { Stream } from "../core/stream";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function pump<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$pump",
>(options?: Omit<Stream.Options<VALUE, NAME>, "source">): Transform<INPUT, NAME, Stream<VALUE, NAME>> {
  return (input) => {
    const { name, terminate, ...rest } = options ?? {};

    const output = new Stream({
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
