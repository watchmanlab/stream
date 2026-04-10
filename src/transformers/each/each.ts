import { Stream } from "../../streams/index.ts";

const NAME = "each";

class Each<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = each.Name,
> extends Stream<
  | Stream.ExtractValue<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<Each<INPUT_STREAM, CLEAN_VALUE, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  constructor(name: NAME, inputStream: INPUT_STREAM, callback: each.Callback<CLEAN_VALUE, ERROR>) {
    super(name, async function* () {
      for await (const value of inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        try {
          const maybePromise = callback(value);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            yield Stream.sourceErr({
              value,
              error: result.value,
              source: self,
            }) as never;
            continue;
          }
          if (result) {
            yield result;
            continue;
          }
          yield value;
        } catch (error) {
          yield Stream.sourceErr({ value, error: error, source: self }) as never;
        }
      }
    });

    const self = Stream.traversable(this, inputStream);
  }
}

export function each<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
>(
  name: NAME,
  callback: each.Callback<CLEAN_VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Each<INPUT_STREAM, CLEAN_VALUE, ERROR, NAME>, INPUT_STREAM>>;
export function each<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
>(
  callback: each.Callback<CLEAN_VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Each<INPUT_STREAM, CLEAN_VALUE, ERROR, each.Name>, INPUT_STREAM>>;
export function each<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = each.Name,
>(
  nameOrCallback: NAME | each.Callback<CLEAN_VALUE, ERROR>,
  callback?: each.Callback<CLEAN_VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Each<INPUT_STREAM, CLEAN_VALUE, ERROR, NAME>, INPUT_STREAM>> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrCallback === "string"
        ? new Each(nameOrCallback, inputStream, callback!)
        : new Each(NAME as NAME, inputStream, nameOrCallback),
      inputStream,
    );
}

export namespace each {
  export type Name = typeof NAME;
  export type Callback<CLEAN_VALUE, ERROR> = (
    value: CLEAN_VALUE,
  ) =>
    | void
    | Stream.Err<ERROR>
    | Stream.Terminate
    | Stream.Skip
    | Promise<void | Stream.Err<ERROR> | Stream.Terminate | Stream.Skip>;
}
