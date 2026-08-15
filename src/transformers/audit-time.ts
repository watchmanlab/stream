import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class AuditTime<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$auditTime",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, ms: number, options?: Stream.Options<VALUE, NAME>) {
    let latest: VALUE;
    let timer: any = null;

    const inputConsumer = input.consume((self, value) => {
      latest = value;
      if (!timer) {
        timer = setTimeout(() => {
          this.push(latest);
          timer = null;
        }, ms);
      }
    });

    super(input, {
      ...options,
      name: options?.name ?? ("$auditTime" as NAME),
      next(self, consumer) {
        inputConsumer.next();
        options?.next?.(self, consumer);
      },
      terminate(self, reason) {
        clearTimeout(timer);
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
      },
    });
  }
}

export function auditTime<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$auditTime",
>(ms: number, options?: Stream.Options<VALUE, NAME>): Transform<INPUT, AuditTime<INPUT, VALUE, NAME>> {
  return (input) => new AuditTime(input, ms, options);
}
