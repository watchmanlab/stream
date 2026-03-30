import { Stream } from "../../streams";

const NAME = "error";

export class Error<
  INPUT_STREAM extends Stream.AnyStream,
  SOURCE_ERR extends Stream.AnySourceErr = Stream.ExtractSourceErr<INPUT_STREAM>,
  SOURCE_ERR_STREAM extends Stream.AnyStream = Stream.ExtractSourceErrStream<SOURCE_ERR>,
  SOURCE_ERR_VALUE = Stream.ExtractValue<SOURCE_ERR>,
  ERROR = Stream.ExtractError<SOURCE_ERR>,
  NAME extends string = error.Name,
> extends Stream<Stream.ExtractValue<INPUT_STREAM>, NAME> {
  protected _expected?: Stream<Stream.SourceErr<SOURCE_ERR_VALUE, ERROR, SOURCE_ERR_STREAM>, `${NAME}Expected`>;
  protected _unexpected?: Stream<Stream.SourceErr<SOURCE_ERR_VALUE, unknown, SOURCE_ERR_STREAM>, `${NAME}Unexpected`>;

  constructor(name: NAME, inputStream: INPUT_STREAM) {
    super(name, async function* () {
      for await (const value of inputStream) {
        if (Stream.isSourceErr<SOURCE_ERR_VALUE, ERROR, SOURCE_ERR_STREAM>(value)) {
          if (Stream.isErr<ERROR>(value.error)) {
            self._expected?.push(value);
          } else {
            self._unexpected?.push(value);
          }
        }
        yield value;
      }
    });
    const self = this;
  }

  get expected() {
    if (!this._expected) this._expected = new Stream(`${this._name}ExpectedErrors` as never);
    return this._expected;
  }
  get unexpected() {
    if (!this._unexpected) this._unexpected = new Stream(`${this._name}UnexpectedErrors` as never);
    return this._unexpected;
  }
}

export function error<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  SOURCE_ERR extends Stream.AnySourceErr = Stream.ExtractSourceErr<INPUT_STREAM>,
  SOURCE_ERR_STREAM extends Stream.AnyStream = Stream.ExtractSourceErrStream<SOURCE_ERR>,
  SOURCE_ERR_VALUE = Stream.ExtractValue<SOURCE_ERR>,
  ERROR = Stream.ExtractError<SOURCE_ERR>,
>(
  name: NAME,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Error<INPUT_STREAM, SOURCE_ERR, SOURCE_ERR_STREAM, SOURCE_ERR_VALUE, ERROR, NAME>, INPUT_STREAM>
>;
export function error<
  INPUT_STREAM extends Stream.AnyStream,
  SOURCE_ERR extends Stream.AnySourceErr = Stream.ExtractSourceErr<INPUT_STREAM>,
  SOURCE_ERR_STREAM extends Stream.AnyStream = Stream.ExtractSourceErrStream<SOURCE_ERR>,
  SOURCE_ERR_VALUE = Stream.ExtractValue<SOURCE_ERR>,
  ERROR = Stream.ExtractError<SOURCE_ERR>,
>(): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<
    Error<INPUT_STREAM, SOURCE_ERR, SOURCE_ERR_STREAM, SOURCE_ERR_VALUE, ERROR, error.Name>,
    INPUT_STREAM
  >
>;

export function error<
  INPUT_STREAM extends Stream.AnyStream,
  SOURCE_ERR extends Stream.AnySourceErr = Stream.ExtractSourceErr<INPUT_STREAM>,
  SOURCE_ERR_STREAM extends Stream.AnyStream = Stream.ExtractSourceErrStream<SOURCE_ERR>,
  SOURCE_ERR_VALUE = Stream.ExtractValue<SOURCE_ERR>,
  ERROR = Stream.ExtractError<SOURCE_ERR>,
  NAME extends string = error.Name,
>(
  name = NAME as NAME,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Error<INPUT_STREAM, SOURCE_ERR, SOURCE_ERR_STREAM, SOURCE_ERR_VALUE, ERROR, NAME>, INPUT_STREAM>
> {
  return (inputStream) => Stream.traversable(new Error(name, inputStream), inputStream);
}

export namespace error {
  export type Name = typeof NAME;
}
