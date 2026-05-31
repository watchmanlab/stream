import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Take<INPUT extends Mitto.AnyMitto, NAME extends string = take.Name> extends Transformer<
  INPUT,
  Mitto.ExtractValue<INPUT>,
  NAME
> {
  constructor(name = take.NAME as NAME, input: INPUT, count: number) {
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          this.emit(value);
          if (--count === 0) this.abort();
        });

        return () => signal.emit();
      },
    });
  }
}

export function take<INPUT extends Mitto.AnyMitto, NAME extends string = take.Name>(
  count: number,
): Mitto.Transform<INPUT, NAME, Take<INPUT, NAME>> {
  return (input, name) => new Take(name, input, count);
}

export namespace take {
  export const NAME = "take";
  export type Name = typeof NAME;
}
