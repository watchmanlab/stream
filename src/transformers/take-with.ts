import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class TakeWith<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$takeWith",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, notifier: AnyStream, options?: Stream.Options<VALUE, NAME>) {
    options = { ...options };

    const notifierConsumer = notifier.consume((self) => this.terminate("complete")).next();

    const inputConsumer = input.consume((self, value) => this.push(value));
    super(input, {
      ...options,
      name: options?.name ?? ("$takeWith" as NAME),
      scope: [notifier],
      next(self, consumer) {
        options?.next?.(self, consumer);
        inputConsumer.next();
      },
      terminate(self, reason) {
        notifierConsumer.terminate(reason);
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
      },
    });
  }
}

export function takeWith<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$takeWith",
>(notifier: AnyStream, options?: Stream.Options<VALUE, NAME>): Transform<INPUT, TakeWith<INPUT, VALUE, NAME>> {
  return (input) => new TakeWith(input, notifier, options);
}

export namespace TakeWith {
  export type Predicate<VALUE> = (value: VALUE) => boolean;
}
