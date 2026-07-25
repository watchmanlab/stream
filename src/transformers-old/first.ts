import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class First<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = first.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(
    name = first.NAME as NAME,
    input: INPUT,
    public readonly predicate?: first.Predicate<VALUE>,
  ) {
    super(name, input, {
      source: () => {
        const signal = input.consume((value) => {
          if (!predicate || predicate(value)) {
            this.emit(value);
            this.abort();
          }
        });

        return () => signal.emit();
      },
    });
  }
}

export function first<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = first.Name,
>(predicate?: first.Predicate<VALUE>): Mitto.Transform<INPUT, NAME, First<INPUT, VALUE, NAME>> {
  return (input, name) => new First(name, input, predicate);
}

export namespace first {
  export const NAME = "first";
  export type Name = typeof NAME;
  export type Predicate<VALUE> = (value: VALUE) => boolean;
}
