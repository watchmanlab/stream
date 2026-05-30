import { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Map<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
> extends Transformer<INPUT, MAPPED, NAME> {
  constructor(name = map.NAME as NAME, input: INPUT, callback: map.Callback<VALUE, MAPPED>) {
    super(name, input, {
      source: () => input.listen((value) => this.emit(callback(value))).emit.bind(this),
    });
  }
}
export function map<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
>(callback: map.Callback<VALUE, MAPPED>): Mitto.Transform<INPUT, NAME, Map<INPUT, VALUE, MAPPED, NAME>> {
  return (input, name) => new Map(name, input, callback);
}
export namespace map {
  export const NAME = "map";
  export type Name = typeof NAME;
  export type Callback<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
