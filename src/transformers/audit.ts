import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Audit<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$audit",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, notifier: AnyStream, options?: Stream.Options<VALUE, NAME>) {
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
      scope: notifier,
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
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$audit",
>(notifier: AnyStream, options?: Stream.Options<VALUE, NAME>): Transform<INPUT, Audit<INPUT, VALUE, NAME>> {
  return (input) => new Audit(input, notifier, options);
}
