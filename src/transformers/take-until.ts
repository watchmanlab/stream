import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class TakeUntil<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$takeUntil",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, predicate: TakeUntil.Predicate<VALUE>, options?: Stream.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((_, value) => {
      if (predicate(value)) {
        this.terminate("complete");
      } else {
        this.push(value);
      }
    });

    super(input, {
      ...rest,
      name: name ?? ("$takeUntil" as NAME),
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

export function takeUntil<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$takeUntil",
>(
  predicate: TakeUntil.Predicate<VALUE>,
  options?: Stream.Options<VALUE, NAME>,
): Transform<INPUT, TakeUntil<INPUT, VALUE, NAME>> {
  return (input) => new TakeUntil(input, predicate, options);
}

export namespace TakeUntil {
  export type Predicate<VALUE> = (value: VALUE) => boolean;
}
