import { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Distinct<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = distinct.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(name = distinct.NAME as NAME, input: INPUT) {
    let last: VALUE;
    super(name, input, {
      source: () => {
        const signal = input.consume((value) => {
          if (value !== last) {
            last = value;
            this.emit(value);
          }
        });
        return () => signal.emit();
      },
    });
  }
}

export function distinct<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = distinct.Name,
>(): Mitto.Transform<INPUT, NAME, Distinct<INPUT, VALUE, NAME>> {
  return (input, name) => new Distinct(name, input);
}

export namespace distinct {
  export const NAME = "distinct";
  export type Name = typeof NAME;
}
