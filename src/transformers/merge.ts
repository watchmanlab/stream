import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Merge<
  INPUT extends AnyStream,
  OTHER extends AnyStream,
  VALUE extends ExtractValue<INPUT> | ExtractValue<OTHER> = ExtractValue<INPUT> | ExtractValue<OTHER>,
  NAME extends NonEmptyString = "$merge",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, other: OTHER, options?: Stream.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const pending = new Array(2).fill(false);

    const inputConsumer = input.consume((_, value) => {
      pending[0] = false;
      this.push(value);
    });
    const otherConsumer = other.consume((_, value) => {
      pending[1] = false;
      this.push(value);
    });

    super(input, {
      ...rest,
      name: name ?? ("$merge" as NAME),
      next(self, consumer) {
        if (!pending[0]) {
          pending[0] = true;
          inputConsumer.next();
        }
        if (!pending[1]) {
          pending[1] = true;
          otherConsumer.next();
        }
        next?.(self, consumer);
      },
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        otherConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });
  }
}

export function merge<
  INPUT extends AnyStream,
  OTHER extends AnyStream,
  VALUE extends ExtractValue<INPUT> | ExtractValue<OTHER> = ExtractValue<INPUT> | ExtractValue<OTHER>,
  NAME extends NonEmptyString = "$merge",
>(other: OTHER, options?: Stream.Options<VALUE, NAME>): Transform<INPUT, Merge<INPUT, OTHER, VALUE, NAME>> {
  return (input) => new Merge(input, other, options);
}
