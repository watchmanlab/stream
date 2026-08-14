import { Producer } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Audit<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$audit",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, notifier: AnyProducer, options?: Producer.Options<VALUE, NAME>) {
    let latest: VALUE;
    const notifierConsumer = notifier
      .consume((self) => {
        this.push(latest);
        self.next();
      })
      .next();

    const inputConsumer = input
      .consume((self, value) => {
        latest = value;
        self.next();
      })
      .next();

    super(input, {
      ...options,
      scope: [notifier],
      name: options?.name ?? ("$audit" as NAME),
      terminate(self, reason) {
        notifierConsumer.terminate(reason);
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
      },
    });
  }
}

export function audit<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$audit",
>(notifier: AnyProducer, options?: Producer.Options<VALUE, NAME>): Transform<INPUT, Audit<INPUT, VALUE, NAME>> {
  return (input) => new Audit(input, notifier, options);
}
