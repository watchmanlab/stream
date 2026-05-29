import { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Map<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
> extends Transformer<INPUT, MAPPED, NAME> {
  constructor({ name, input, callback }: map.Options<INPUT, VALUE, MAPPED, NAME>) {
    super({
      name: name ?? (map.NAME as NAME),
      input: input,
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
  return (input, name) => new Map({ name, input, callback });
}
export namespace map {
  export const NAME = "map";
  export type Name = typeof NAME;
  export type Callback<VALUE, MAPPED> = (value: VALUE) => MAPPED;
  export type Options<
    INPUT extends Mitto.AnyMitto,
    VALUE extends Mitto.ExtractValue<INPUT>,
    MAPPED,
    NAME extends string,
  > = {
    name?: NAME;
    input: INPUT;
    callback: Callback<VALUE, MAPPED>;
  };
}
