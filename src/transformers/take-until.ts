import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class TakeUntil<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = takeUntil.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(name = takeUntil.NAME as NAME, input: INPUT, predicate: takeUntil.Predicate<VALUE>) {
    super(name, input, {
      source: () =>
        this.listen((value) => {
          if (predicate(value)) {
            this.abort();
            return;
          }

          this.emit(value);
        }).emit.bind(this),
    });
  }
}

export function takeUntil<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = takeUntil.Name,
>(predicate: takeUntil.Predicate<VALUE>): Mitto.Transform<INPUT, NAME, TakeUntil<INPUT, VALUE, NAME>> {
  return (input, name) => new TakeUntil(name, input, predicate);
}

export namespace takeUntil {
  export const NAME = "takeUntil";
  export type Name = typeof NAME;
  export type Predicate<VALUE> = (value: VALUE) => boolean;
}
