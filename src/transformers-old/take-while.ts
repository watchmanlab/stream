import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class TakeWhile<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = takeWhile.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(
    name = takeWhile.NAME as NAME,
    input: INPUT,
    public readonly predicate: takeWhile.Predicate<VALUE>,
  ) {
    super(name, input, {
      source: () => {
        const signal = input.consume((value) => {
          if (!predicate(value)) {
            this.abort();
            return;
          }

          this.emit(value);
        });

        return () => signal.emit();
      },
    });
  }
}

export function takeWhile<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = takeWhile.Name,
>(predicate: takeWhile.Predicate<VALUE>): Mitto.Transform<INPUT, NAME, TakeWhile<INPUT, VALUE, NAME>> {
  return (input, name) => new TakeWhile(name, input, predicate);
}

export namespace takeWhile {
  export const NAME = "takeWhile";
  export type Name = typeof NAME;
  export type Predicate<VALUE> = (value: VALUE) => boolean;
}
