import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Derive<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = derive.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor({ name, input }: derive.Options<INPUT, VALUE, NAME>) {
    super({
      name: name ?? (derive.NAME as NAME),
      input,
      source: () => input.listen((value) => this.emit(value)).emit.bind(this),
    });
  }
}

export function derive<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = derive.Name,
>(): Mitto.Transform<INPUT, NAME, Derive<INPUT, VALUE, NAME>> {
  return (input, name) => new Derive({ name, input });
}

export namespace derive {
  export const NAME = "derive";
  export type Name = typeof NAME;

  export type Options<INPUT extends Mitto.AnyMitto, VALUE extends Mitto.ExtractValue<INPUT>, NAME extends string> = {
    name?: NAME;
    input: INPUT;
  };
}
