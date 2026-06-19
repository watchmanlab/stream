import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Scan<
  INPUT extends Mitto.AnyMitto,
  ACC,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = scan.Name,
> extends Transformer<INPUT, ACC, NAME> {
  constructor(
    name = scan.NAME as NAME,
    input: INPUT,
    public readonly seed: ACC,
    public readonly fn: scan.Fn<ACC, VALUE>,
  ) {
    let acc = seed;
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          acc = fn(acc, value);
          this.emit(acc);
        });

        return () => signal.emit();
      },
    });
  }
}

export function scan<
  INPUT extends Mitto.AnyMitto,
  ACC,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = scan.Name,
>(seed: ACC, fn: scan.Fn<ACC, VALUE>): Mitto.Transform<INPUT, NAME, Scan<INPUT, ACC, VALUE, NAME>> {
  return (input, name) => new Scan(name, input, seed, fn);
}

export namespace scan {
  export const NAME = "scan";
  export type Name = typeof NAME;

  export type Fn<ACC, VALUE> = (acc: ACC, value: VALUE) => ACC;
}
