import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Take<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$take",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, count: number, options?: Stream.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((_, value) => {
      if (count--) {
        this.push(value);
      } else {
        this.terminate("complete");
      }
    });

    super(input, {
      ...rest,
      name: name ?? ("$take" as NAME),
      next(self, consumer) {
        inputConsumer.next();
        next?.(self, consumer);
      },
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        terminate?.(self, reason);
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
