import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Tap<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = tap.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(
    name = tap.NAME as NAME,
    input: INPUT,
    public readonly fn: tap.Fn<VALUE>,
  ) {
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          fn(value);
          this.emit(value);
        });

        return () => signal.emit();
      },
    });
  }
}

export function tap<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = tap.Name,
>(fn: tap.Fn<VALUE>): Mitto.Transform<INPUT, NAME, Tap<INPUT, VALUE, NAME>> {
  return (input, name) => new Tap(name, input, fn);
}

export namespace tap {
  export const NAME = "tap";
  export type Name = typeof NAME;
  export type Fn<VALUE> = (value: VALUE) => void;
}
