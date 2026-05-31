import { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Distinct<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = distinct.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  private _latest: VALUE | Mitto.Empty = Mitto.EMPTY;
  constructor(name = distinct.NAME as NAME, input: INPUT) {
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          if (value !== this._latest) {
            this._latest = value;
            this.emit(value);
          }
        });
        return () => signal.emit();
      },
    });
  }
  get latest() {
    return this._latest;
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
