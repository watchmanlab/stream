import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Debounce<INPUT extends Mitto.AnyMitto, NAME extends string = debounce.Name> extends Transformer<
  INPUT,
  Mitto.ExtractValue<INPUT>,
  NAME
> {
  constructor(name = debounce.NAME as NAME, input: INPUT, ms: number) {
    let timer: any = null;
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          clearTimeout(timer);
          timer = setTimeout(() => this.emit(value), ms);
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

export function debounce<INPUT extends Mitto.AnyMitto, NAME extends string = debounce.Name>(
  ms: number,
): Mitto.Transform<INPUT, NAME, Debounce<INPUT, NAME>> {
  return (input, name) => new Debounce(name, input, ms);
}

export namespace debounce {
  export const NAME = "debounce";
  export type Name = typeof NAME;
}
