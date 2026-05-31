import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Delay<INPUT extends Mitto.AnyMitto, NAME extends string = delay.Name> extends Transformer<
  INPUT,
  Mitto.ExtractValue<INPUT>,
  NAME
> {
  constructor(name = delay.NAME as NAME, input: INPUT, ms: number) {
    let timer: any = null;
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          timer = setTimeout(() => this.emit(value), ms);
        });

        return () => signal.emit();
      },
      aborted: () => clearTimeout(timer),
    });
  }
}

export function delay<INPUT extends Mitto.AnyMitto, NAME extends string = delay.Name>(
  ms: number,
): Mitto.Transform<INPUT, NAME, Delay<INPUT, NAME>> {
  return (input, name) => new Delay(name, input, ms);
}

export namespace delay {
  export const NAME = "delay";
  export type Name = typeof NAME;
}
