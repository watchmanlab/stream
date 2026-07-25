import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Flat<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  DEPTH extends number = 0,
  NAME extends string = flat.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(
    name = flat.NAME as NAME,
    input: INPUT,
    public readonly depth = 0 as DEPTH,
  ) {
    super(name, input, {
      source: () => {
        const signal = input.consume((value) => {
          if (Array.isArray(value)) {
            const flatten = depth === 0 ? value : value.flat(depth);
            for (let i = 0, length = flatten.length; i < length; i++) {
              this.emit(flatten[i]);
            }
          } else {
            this.emit(value as never);
          }
        });

        return () => signal.emit();
      },
    });
  }
}

export function flat<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  DEPTH extends number = 0,
  NAME extends string = flat.Name,
>(depth = 0 as DEPTH): Mitto.Transform<INPUT, NAME, Flat<INPUT, VALUE, DEPTH, NAME>> {
  return (input, name) => new Flat(name, input, depth);
}

export namespace flat {
  export const NAME = "flat";
  export type Name = typeof NAME;
}
