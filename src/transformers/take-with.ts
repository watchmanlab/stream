import { Producer } from "../core/producer";
import { Transformer } from "../core/transformer";
import { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";
import { merge } from "./merge";

export class TakeWith<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$takeWith",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, $notifier: AnyProducer, options?: Producer.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((_, value) => this.push(value));

    super(input, {
      ...rest,
      name: name ?? ("$takeWith" as NAME),
      $terminate: $notifier.$terminate.pipe(merge(input.$terminate)),
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

export function takeWith<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$takeWith",
>($notifier: AnyProducer, options?: Producer.Options<VALUE, NAME>): Transform<INPUT, TakeWith<INPUT, VALUE, NAME>> {
  return (input) => new TakeWith(input, $notifier, options);
}

export namespace TakeWith {
  export type Predicate<VALUE> = (value: VALUE) => boolean;
}
