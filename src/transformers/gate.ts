import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { AnyStream, ExtractValue, NonEmptyString } from "../core/types";

export function gate<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$gate",
>(control: Stream<boolean, any>, options?: Omit<Stream.Options<VALUE, NAME>, "source">) {
  return (input: INPUT) => {
    const { name, next, terminate, ...rest } = options ?? {};

    let inputConsumer: Consumer<VALUE> | undefined;

    const output = new Stream<VALUE, NAME>({
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
