import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString } from "../core/types";

export class Range<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$range",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, start: number, offset: number, options?: Stream.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputconsumer = input.consume((self, value) => {
      //
    });

    super(input, {
      ...rest,
      name: name ?? ("$range" as NAME),
      next(self, consumer) {
        inputconsumer.next();
        next?.(self, consumer);
      },
      terminate(self, reason) {
        inputconsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });
  }
}
