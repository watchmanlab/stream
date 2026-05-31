import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Throttle<INPUT extends Mitto.AnyMitto, NAME extends string = throttle.Name> extends Transformer<
  INPUT,
  Mitto.ExtractValue<INPUT>,
  NAME
> {
  constructor(name = throttle.NAME as NAME, input: INPUT, ms: number) {
    let inThrottle = false;
    let timer: any = null;
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          if (inThrottle) return;

          this.emit(value);
          inThrottle = true;

          timer = setTimeout(() => {
            inThrottle = false;
          }, ms);
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
export function throttle<INPUT extends Mitto.AnyMitto, NAME extends string = throttle.Name>(
  ms: number,
): Mitto.Transform<INPUT, NAME, Throttle<INPUT, NAME>> {
  return (input, name) => new Throttle(name, input, ms);
}
export namespace throttle {
  export const NAME = "throttle";
  export type Name = typeof NAME;
}
