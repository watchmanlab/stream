import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class DistinctBy<
  INPUT extends Mitto.AnyMitto,
  KEY,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = distinctBy.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(name = distinctBy.NAME as NAME, input: INPUT, fn: (value: VALUE) => KEY) {
    let last: KEY;
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          const key = fn(value);
          if (key !== last) {
            last = key;
            this.emit(value);
          }
        });
        return () => signal.emit();
      },
    });
  }
}

export function distinctBy<
  INPUT extends Mitto.AnyMitto,
  KEY,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = distinctBy.Name,
>(fn: (value: VALUE) => KEY): Mitto.Transform<INPUT, NAME, DistinctBy<INPUT, KEY, VALUE, NAME>> {
  return (input, name) => new DistinctBy(name, input, fn);
}

export namespace distinctBy {
  export const NAME = "distinctBy";
  export type Name = typeof NAME;
}
