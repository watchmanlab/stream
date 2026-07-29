import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class AuditCount<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$auditCount",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, count: number, options?: Stream.Options<VALUE, NAME>) {
    let _count = count;

    const inputConsumer = input.consume((self, value) => {
      if (!--_count) {
        _count = count;
        this.push(value);
      } else {
        self.next();
      }
    });

    super(input, {
      ...options,
      name: options?.name ?? ("$auditCount" as NAME),
      next(self, consumer) {
        inputConsumer.next();
        options?.next?.(self, consumer);
      },
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
      },
    });
  }
}

export function auditCount<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$auditCount",
>(count: number, options?: Stream.Options<VALUE, NAME>): Transform<INPUT, AuditCount<INPUT, VALUE, NAME>> {
  return (input) => new AuditCount(input, count, options);
}
