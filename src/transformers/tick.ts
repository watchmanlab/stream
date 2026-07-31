import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Tick<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$tick",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, options?: Stream.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = { ...options };

    let inputConsumer = input.consume((_, value) => this.push(value));

    super(input, {
      ...rest,
      name: name ?? ("$tick" as NAME),
      next(self, consumer) {
        setTimeout(() => {
          inputConsumer.next();
          next?.(self, consumer);
        });
      },
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });
  }
}

export function tick<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$tick",
>(options?: Stream.Options<VALUE, NAME>): Transform<INPUT, Tick<INPUT, VALUE, NAME>> {
  return (input) => new Tick(input, options);
}
