import { Stream } from "../core/stream";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function pump<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$pump",
>(): Transform<INPUT, VALUE, NAME, Stream<VALUE, NAME>> {
  return (input, options) => {
    const inputConsumer = input
      .consume((self, value) => {
        output.push(value);
        self.next();
      })
      .next();
    const output = new Stream({
      ...options,
      name: options?.name ?? ("$pump" as NAME),
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
      },
    });

    return output;
  };
}
