import { Stream } from "../../streams";
import { catchError } from "../catch-error";
import { map, Map } from "../map";
import { pump } from "../pump";

const NAME = "each";

export class Each<VALUE, ERROR, NAME extends string = each.Name> extends Stream<VALUE, NAME> {
  private _map: Map<VALUE, VALUE, ERROR>;
  protected _errors?: Stream<map.ErrorEvent<ERROR, this>, `${NAME}Errors`>;

  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      const generator = self._map[Symbol.asyncIterator]();
      let next = await generator.next();
      try {
        while (!next.done) {
          const feedback = yield next.value;
          next = await generator.next(feedback);
        }
      } finally {
        await generator.return();
      }
    });
    const self = this;

    this._map = new Map<VALUE, VALUE, ERROR>(source, undefined, async (value, _, compensate) => {
      const result = await callback(value, this, compensate);
      if (Stream.Result.isErr(result)) return result;

      return value;
    });
  }

  get errors() {
    if (!this._errors)
      this._errors = this._map.errors.pipe(
        `${this._name}Errors`,
        map((error) => ({ ...error, self: this })),
      );

    return this._errors;
  }
}
export function each<VALUE, ERROR, NAME extends string = each.Name>(
  callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Each<VALUE, ERROR, NAME>> {
  return (_, source, name) => new Each(source, name, callback);
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<VALUE, ERROR, SELF extends Stream<VALUE, any>> = (
    value: VALUE,
    self: SELF,
    compensate: map.Compensate,
  ) => void | Stream.Result.Err<ERROR> | Promise<void | Stream.Result.Err<ERROR>>;
}
