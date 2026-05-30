import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class SkipUntil<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = skipUntil.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(name = skipUntil.NAME as NAME, input: INPUT, predicate: skipUntil.Predicate<VALUE>) {
    super(name, input, {
      source: () =>
        this.listen((value) => {
          if (!predicate(value)) return;

          this.emit(value);
        }).emit.bind(this),
    });
  }
}

export function skipUntil<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = skipUntil.Name,
>(predicate: skipUntil.Predicate<VALUE>): Mitto.Transform<INPUT, NAME, SkipUntil<INPUT, VALUE, NAME>> {
  return (input, name) => new SkipUntil(name, input, predicate);
}

export namespace skipUntil {
  export const NAME = "skipUntil";
  export type Name = typeof NAME;
  export type Predicate<VALUE> = (value: VALUE) => boolean;
}
