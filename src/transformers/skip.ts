import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Skip<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$skip",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, count: number, options?: Stream.Options<VALUE, NAME>) {
    options = { ...options };

    let inputConsumer = input.consume((self, value) => {
      if (count--) {
        self.next();
      } else {
        inputConsumer.terminate("complete");
        inputConsumer = input.consume((self, value) => this.push(value));
        this.push(value);
      }
    });

    super(input, {
      ...options,
      name: options?.name ?? ("$skip" as NAME),
      next(self, consumer) {
        options?.next?.(self, consumer);
        inputConsumer.next();
      },
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
      },
    });
  }
}

export function skip<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$skip",
>(count: number, options?: Stream.Options<VALUE, NAME>): Transform<INPUT, Skip<INPUT, VALUE, NAME>> {
  return (input) => new Skip(input, count, options);
}
