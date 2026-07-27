import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Passive<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$passive",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, options?: Stream.Options<VALUE, NAME>) {
    const inputPushConsumer = input.$push.consume((self, value) => this.push(value));

    super(input, {
      ...options,
      name: options?.name ?? ("$passive" as NAME),
      next: (self, consumer) => {
        options?.next?.(self, consumer);
        inputPushConsumer.next();
      },
      terminate(self, reason) {
        inputPushConsumer.terminate(reason);
        options?.terminate?.(self, reason);
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
