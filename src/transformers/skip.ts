import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Skip<INPUT extends Mitto.AnyMitto, NAME extends string = skip.Name> extends Transformer<
  INPUT,
  Mitto.ExtractValue<INPUT>,
  NAME
> {
  constructor(
    name = skip.NAME as NAME,
    input: INPUT,
    public readonly count: number,
  ) {
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          if (--count > 0) return;
          this.emit(value);
        });

        return () => signal.emit();
      },
    });
  }
}

export function skip<INPUT extends Mitto.AnyMitto, NAME extends string = skip.Name>(
  count: number,
): Mitto.Transform<INPUT, NAME, Skip<INPUT, NAME>> {
  return (input, name) => new Skip(name, input, count);
}

export namespace skip {
  export const NAME = "skip";
  export type Name = typeof NAME;
}
