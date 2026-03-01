import { Stream } from "../../streams";
import { catchError } from "../catch-error";
import { map, Map } from "../map";
import { pump } from "../pump";

const NAME = "each";

export class Each<VALUE, ERROR, NAME extends string = each.Name> extends Map<VALUE, VALUE, ERROR, NAME> {
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
  ) {
    super(source, name, async (value, self, compensate) => {
      await callback(value, self, compensate);
      return value;
    });
  }
}
export function each<VALUE, ERROR, NAME extends string = each.Name>(
  callback: each.Callback<VALUE, ERROR, Each<VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Each<VALUE, ERROR, NAME>> {
  return (_, source, name) => new Each(source, name, callback);
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<VALUE, ERROR, SELF extends Each<VALUE, ERROR, any>> = (
    value: VALUE,
    self: SELF,
    compensate: map.Compensate,
  ) => void | Stream.Result.Err<ERROR> | Promise<void | Stream.Result.Err<ERROR>>;
}

new Stream([1, 2, 4])
  .pipe(
    catchError((err) => {
      // console.log(err.error);
    }),
  )

  .pipe(
    map((v, self, compensate) => {
      return v;
    }),
  )
  .pipe(
    "map1",
    map((v) => {
      if (v === 1) return Stream.Result.err("kechmahaja");
      return v.toFixed();
    }),
  )
  .pipe(each((v, self, compensate) => console.log(v)))
  .pipe(pump())
  .each.map1.errors.pipe(each((v, self) => console.log(v.error)))
  .pipe(pump()).each.map1Errors.name;
