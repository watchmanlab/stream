import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Merge<
  INPUT extends Mitto.AnyMitto,
  OTHERS extends [other: Mitto<any>, ...others: Mitto<any>[]],
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = merge.Name,
> extends Transformer<INPUT, VALUE | Mitto.ExtractValue<OTHERS[number]>, NAME> {
  private _others: OTHERS;
  constructor(name = merge.NAME as NAME, input: INPUT, others: OTHERS) {
    super(name, input, {
      source: () => {
        const signals = [input, ...this._others].map((m) => m.listen((value) => this.emit(value)));
        return () => signals.forEach((abort) => abort.emit());
      },
    });

    this._others = [...others];
  }
  get others() {
    return [...this._others];
  }
}

export function merge<
  INPUT extends Mitto.AnyMitto,
  OTHERS extends [other: Mitto<any>, ...others: Mitto<any>[]],
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = merge.Name,
>(...others: OTHERS): Mitto.Transform<INPUT, NAME, Merge<INPUT, OTHERS, VALUE, NAME>> {
  return (input, name) => new Merge(name, input, others);
}

export namespace merge {
  export const NAME = "merge";
  export type Name = typeof NAME;
}
