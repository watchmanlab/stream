import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class AuditTime<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = auditTime.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(name = auditTime.NAME as NAME, input: INPUT, ms: number) {
    let timer: any = null;
    let latest: VALUE;
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          latest = value;
          if (!timer) {
            timer = setTimeout(() => {
              this.emit(latest);
              timer = null;
            }, ms);
          }
        });
        return () => {
          signal.emit();
          clearTimeout(timer);
        };
      },
      aborted: () => clearTimeout(timer),
    });
  }
}

export function auditTime<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = auditTime.Name,
>(ms: number): Mitto.Transform<INPUT, NAME, AuditTime<INPUT, VALUE, NAME>> {
  return (input, name) => new AuditTime(name, input, ms);
}

export namespace auditTime {
  export const NAME = "auditTime";
  export type Name = typeof NAME;
}
