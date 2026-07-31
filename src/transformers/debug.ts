import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Debug<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$debug",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, message?: string, options?: Stream.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((self, value) => {
      message ? console.log(value, message) : console.log(value);
      this.push(value);
      self.next();
    });
    super(input, {
      ...rest,
      name: name ?? ("$debug" as NAME),

      terminate(self, reason) {
        inputConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });
    inputConsumer.next();
  }
}

export function debug<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$debug",
>(message?: string, options?: Stream.Options<VALUE, NAME>): Transform<INPUT, Debug<INPUT, VALUE, NAME>> {
  return (input) => new Debug(input, message, options);
}
