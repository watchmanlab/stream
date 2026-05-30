import { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Filter<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
> extends Transformer<INPUT, FILTERED, NAME> {
  private _filtered?: Mitto<VALUE, `${NAME}Filtered`>;
  constructor(name = filter.NAME as NAME, input: INPUT, predicate: filter.Predicate<VALUE, FILTERED>) {
    super(name, input, {
      source: () => {
        return input
          .listen((value) => {
            if (predicate(value)) {
              this.emit(value);
            } else {
              this._filtered?.emit(value);
            }
          })
          .emit.bind(this);
      },
      aborted: () => this._filtered?.abort(),
    });
  }

  get filtered() {
    if (!this._filtered) this._filtered = new Mitto({ name: `${this.name}Filtered` });
    return this._filtered;
  }
}

export function filter<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
>(predicate: filter.Predicate<VALUE, FILTERED>): Mitto.Transform<INPUT, NAME, Filter<INPUT, VALUE, FILTERED, NAME>> {
  return (input, name) => new Filter(name, input, predicate);
}

export namespace filter {
  export const NAME = "filter";
  export type Name = typeof NAME;
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);
}
