import { Producer } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Skip<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$skip",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, count: number, options?: Producer.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    let inputConsumer = input.consume((self, value) => {
      if (count--) {
        self.next();
      } else {
        inputConsumer.terminate("complete");
        inputConsumer = input.consume((_, value) => this.push(value));
        this.push(value);
      }
    });

    super(input, {
      ...rest,
      name: name ?? ("$skip" as NAME),
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

export function skip<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$skip",
>(count: number, options?: Producer.Options<VALUE, NAME>): Transform<INPUT, Skip<INPUT, VALUE, NAME>> {
  return (input) => new Skip(input, count, options);
}
