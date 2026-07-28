import { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Last<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = last.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(name = last.NAME as NAME, input: INPUT) {
    let last: VALUE;
    super(name, input, {
      source: () => {
        const signal = input.consume((value) => {
          last = value;
        });
        return () => signal.emit();
      },
      abort: () => {
        this.emit(last);
      },
    });
  }
}

export function last<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = last.Name,
>(): Mitto.Transform<INPUT, NAME, Last<INPUT, VALUE, NAME>> {
  return (input, name) => new Last(name, input);
}

export namespace last {
  export const NAME = "last";
  export type Name = typeof NAME;
}
