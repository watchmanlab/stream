import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Tick<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$tick",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, options?: Stream.Options<VALUE, NAME>) {
    options = { ...options };

    let inputConsumer = input.consume((self, value) => this.push(value));
    super(input, {
      ...options,
      name: options?.name ?? ("$tick" as NAME),
      next(self, consumer) {
        queueMicrotask(() => {
          options?.next?.(self, consumer);
          inputConsumer.next();
        });
      },
      terminate(self, reason) {
        queueMicrotask(() => {});
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
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
