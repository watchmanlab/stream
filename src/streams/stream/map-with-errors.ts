import { Stream } from "./stream";

const NAME = "mapWithErrors";

class MapWithErrors<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = mapWithErrors.Name,
> extends Stream<MAPPED, NAME> {
  private _error?: Stream<Stream.TransformError<VALUE, ERROR, this>, `${NAME}Error`>;

  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: mapWithErrors.Mapper<VALUE, MAPPED, ERROR>) {
    super(name);

    const signal = new Stream();

    this.firstListenerAdded.listen(() => {
      inputStream.listen((value) => {
        try {
          const result = mapper(value);

          if (result instanceof Stream.Error) {
            const sourceError = new Stream.TransformError(value, result.value, this);
            if (!this._error?.listenersCount) throw sourceError;
            this._error?.push(sourceError);
          } else {
            this.push(result);
          }
        } catch (error: any) {
          if (!this._error?.listenersCount) throw error;
          this._error?.push(error);
        }
      }, signal);
    });
    this.lastListenerRemoved.listen(() => {
      signal.push();
    });
    this.terminated.listen(() => {
      signal.push();
    });
  }

  get error() {
    if (!this._error) this._error = new Stream(`${this.name}Error`);
    return this._error;
  }
}

export function mapWithErrors<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = mapWithErrors.Name,
>(
  mapper: mapWithErrors.Mapper<VALUE, MAPPED, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, MapWithErrors<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>> {
  return (inputStream, name) => Stream.transformer(new MapWithErrors(name, inputStream, mapper), inputStream);
}

export namespace mapWithErrors {
  export type Name = typeof NAME;

  export type Mapper<VALUE, MAPPED, ERROR> = (value: VALUE) => MAPPED | Stream.Error<ERROR>;
}
