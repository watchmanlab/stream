import { Stream } from "../../streams/index.ts";
import { Map, map } from "../map/map.ts";

const NAME = "each";

export function each<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
>(
  name: NAME,
  callback: map.Mapper<CLEAN_VALUE, void, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>, INPUT_STREAM>
>;
export function each<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
>(
  callback: map.Mapper<CLEAN_VALUE, void, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, CLEAN_VALUE, ERROR, each.Name>, INPUT_STREAM>
>;

export function each<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = each.Name,
>(
  nameOrCallback: NAME | map.Mapper<CLEAN_VALUE, void, ERROR>,
  callback?: map.Mapper<CLEAN_VALUE, void, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>, INPUT_STREAM>
> {
  const { name, fn } =
    typeof nameOrCallback === "string"
      ? { name: nameOrCallback, fn: callback! }
      : { name: NAME as NAME, fn: nameOrCallback };
  return (inputStream: INPUT_STREAM) =>
    inputStream.pipe(
      map(name, async (v) => {
        const maybePromise = fn(v);
        const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;
        if (Stream.isErr(result)) return result;

        return v;
      }),
    );
}

export namespace each {
  export type Name = typeof NAME;
}
