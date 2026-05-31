import { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Pairwise<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = pairwise.Name,
> extends Transformer<INPUT, [prev: VALUE, curr: VALUE], NAME> {
  constructor(name = pairwise.NAME as NAME, input: INPUT) {
    let prev: VALUE | Mitto.Empty;
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          if (prev !== Mitto.EMPTY) this.emit([prev, value]);
          prev = value;
        });

        return () => signal.emit();
      },
    });
  }
}

export function pairwise<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = pairwise.Name,
>(): Mitto.Transform<INPUT, NAME, Pairwise<INPUT, VALUE, NAME>> {
  return (input, name) => new Pairwise(name, input);
}

export namespace pairwise {
  export const NAME = "pairwise";
  export type Name = typeof NAME;
  export type Fn<VALUE> = (value: VALUE) => void;
}
