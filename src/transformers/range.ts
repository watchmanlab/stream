import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString } from "../core/types";

export class Range<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$range",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, start: number, offset: number, options?: Stream.Options<VALUE, NAME>) {
    options = { ...options };

    const inputconsumer = input.consume((self, value) => {
      //
    });

    super(input, {
      ...options,
      name: options.name ?? ("$range" as NAME),
      next(self, consumer) {
        options.next?.(self, consumer);
        inputconsumer.next();
      },
      terminate(self, reason) {
        inputconsumer.terminate(reason);
        options.terminate?.(self, reason);
      },
    });
  }
}
