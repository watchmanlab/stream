import { Stream } from "../../streams/index.ts";

const NAME = "each";
export class Each<
  INPUT_STREAM extends Stream.AnyStream,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = each.Name,
> extends Stream<
  | Stream.ExtractValue<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<Each<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
    >,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR>, `${NAME}Errors`>;

  constructor(
    options: Stream.TransformOptions<INPUT_STREAM, NAME> & {
      callback: each.Callback<CLEAN_VALUE, ERROR>;
    },
  ) {
    super(options.name ?? (NAME as NAME), async function* () {
      for await (const value of options.inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        try {
          const maybePromise = options.callback(value);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._errors?.push({ type: "expected", value, error: result.value });
            yield Stream.sourceErr({ value, error: result.value, source: self }) as never;
            continue;
          }

          yield value;
        } catch (error) {
          if (Stream.isErr<ERROR>(error)) {
            self._errors?.push({ type: "expected", value, error: error.value });
            yield Stream.sourceErr({ value, error: error.value, source: self }) as never;
          } else {
            self._errors?.push({ type: "unexpected", value, error: error });
            yield Stream.sourceErr({ value, error: error, source: self }) as never;
          }
        }
      }
    });

    const self = this;
  }
  get errors() {
    if (!this._errors) this._errors = new Stream(`${this._name}Errors` as never);
    return this._errors;
  }
}
export function each<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = each.Name,
>(
  callback: each.Callback<CLEAN_VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Each<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, ERROR, NAME>> {
  return (options) => new Each({ ...options, callback });
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<CLEAN_VALUE, ERROR> = (
    value: CLEAN_VALUE,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}
