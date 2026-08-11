import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";

import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function passive<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$passive",
>(options?: Stream.Options<VALUE, NAME>): Transform<INPUT, NAME, Stream<VALUE, NAME>> {
  return (input) => {
    const { name, consumerJoin, next, terminate, ...rest } = options ?? {};

    let inputConsumer: Consumer<VALUE>;

    const output = new Stream({
      ...rest,
      name: name ?? ("$passive" as NAME),
      consumerJoin(self, consumer) {
        if (self.consumersCount === 1) {
          inputConsumer = input.consume((_, value) => self.push(value), {
            terminate(_, reason) {
              self.terminate(reason);
            },
          });
        }
        consumerJoin?.(self, consumer);
      },
      next(self, consumer) {
        next?.(self, consumer);
      },
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });

    return output;
  };
}
