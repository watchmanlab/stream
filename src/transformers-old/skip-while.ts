import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class SkipWhile<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = skipWhile.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(
    name = skipWhile.NAME as NAME,
    input: INPUT,
    public readonly predicate: skipWhile.Predicate<VALUE>,
  ) {
    super(name, input, {
      source: () => {
        const signal = input.consume((value) => {
          if (predicate(value)) return;

          this.emit(value);
        });

        return () => signal.emit();
      },
    });
  }
}

export function skipWhile<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = skipWhile.Name,
>(predicate: skipWhile.Predicate<VALUE>): Mitto.Transform<INPUT, NAME, SkipWhile<INPUT, VALUE, NAME>> {
  return (input, name) => new SkipWhile(name, input, predicate);
}

export namespace skipWhile {
  export const NAME = "skipWhile";
  export type Name = typeof NAME;
  export type Predicate<VALUE> = (value: VALUE) => boolean;
}
