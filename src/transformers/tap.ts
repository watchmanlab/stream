import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Tap<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$tap",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, fn: (value: VALUE, INPUT: INPUT) => void, options?: Stream.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((_, value) => {
      fn(value, input);
      this.push(value);
    });

    super(input, {
      ...rest,
      name: name ?? ("$tap" as NAME),
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

export function tap<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$tap",
>(
  fn: (value: VALUE, INPUT: INPUT) => void,
  options?: Stream.Options<VALUE, NAME>,
): Transform<INPUT, Tap<INPUT, VALUE, NAME>> {
  return (input) => new Tap(input, fn, options);
}
