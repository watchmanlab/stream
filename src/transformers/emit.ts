import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Emit<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = emit.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(name = emit.NAME as NAME, input: INPUT, values: [value: VALUE, ...values: VALUE[]]) {
    super(name, input, {
      source: () => {
        this.emit(...values);
        return input.listen((value) => this.emit(value)).emit.bind(this);
      },
    });
  }
}

export function emit<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = emit.Name,
>(...values: [value: VALUE, ...values: VALUE[]]): Mitto.Transform<INPUT, NAME, Emit<INPUT, VALUE, NAME>> {
  return (input, name) => new Emit(name, input, values);
}

export namespace emit {
  export const NAME = "emit";
  export type Name = typeof NAME;
}
