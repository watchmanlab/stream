import { Producer } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class TakeWhile<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$takeWhile",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, predicate: TakeWhile.Predicate<VALUE>, options?: Producer.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((_, value) => {
      if (predicate(value)) {
        this.push(value);
      } else {
        this.terminate("complete");
      }
    });

    super(input, {
      ...rest,
      name: name ?? ("$takeWhile" as NAME),
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

export function takeWhile<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$takeWhile",
>(
  predicate: TakeWhile.Predicate<VALUE>,
  options?: Producer.Options<VALUE, NAME>,
): Transform<INPUT, TakeWhile<INPUT, VALUE, NAME>> {
  return (input) => new TakeWhile(input, predicate, options);
}

export namespace TakeWhile {
  export type Predicate<VALUE> = (value: VALUE) => boolean;
}
