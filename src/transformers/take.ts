import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Take<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$take",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, count: number, options?: Stream.Options<VALUE, NAME>) {
    options = { ...options };

    const inputConsumer = input.consume((self, value) => {
      this.push(value);
    });

    super(input, {
      ...options,
      name: options?.name ?? ("$take" as NAME),
      next(self, consumer) {
        if (!count--) {
          self.terminate("complete");
        } else {
          options?.next?.(self, consumer);
          inputConsumer.next();
        }
      },
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
      },
    });
  }
}

export function take<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$take",
>(count: number, options?: Stream.Options<VALUE, NAME>): Transform<INPUT, Take<INPUT, VALUE, NAME>> {
  return (input) => new Take(input, count, options);
}
