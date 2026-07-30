import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Passive<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$passive",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, options?: Stream.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputPushConsumer = input.$push.consume((_, value) => this.push(value));

    super(input, {
      ...rest,
      name: name ?? ("$passive" as NAME),
      next: (self, consumer) => {
        inputPushConsumer.next();
        next?.(self, consumer);
      },
      terminate(self, reason) {
        inputPushConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });
  }
}

export function passive<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$passive",
>(options?: Stream.Options<VALUE, NAME>): Transform<INPUT, Passive<INPUT, VALUE, NAME>> {
  return (input) => new Passive(input, options);
}
