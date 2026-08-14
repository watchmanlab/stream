import { Producer } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Debug<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$debug",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, tag?: string, options?: Producer.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((self, value) => {
      tag ? console.log(`${tag}:`, value) : console.log(value);
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
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$debug",
>(tag?: string, options?: Producer.Options<VALUE, NAME>): Transform<INPUT, Debug<INPUT, VALUE, NAME>> {
  return (input) => new Debug(input, tag, options);
}
