import { Stream } from "./stream";

const NAME = "map";

class Map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = map.Name,
> extends Stream<MAPPED, NAME> {
  private _error?: Stream<{ value: VALUE; error: ERROR }, `${NAME}Error`>;

  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<VALUE, MAPPED, ERROR>) {
    super(name);

    const signal = new Stream();

    this.afterFirstListenerAdded.listen(() => {
      inputStream.listen((value) => {
        try {
          const result = mapper(value);

          if (result instanceof Stream.Error) {
            const sourceError = new Stream.TransformError(value, result.value, this);
            if (!this._error?.listenersCount) throw sourceError;
            this._error?.push(sourceError);
          } else {
            this.push(result);
            // await Promise.all(this.push(result));
          }
        } catch (error: any) {
          if (!this._error?.listenersCount) throw error;
          this._error?.push(error);
        }
      }, signal);
    });
    this.afterLastListenerRemoved.listen(() => {
      signal.push();
    });
    this.afterTerminate.listen(() => {
      signal.push();
    });
  }

  get error() {
    if (!this._error) this._error = new Stream(`${this.name}Error`);
    return this._error;
  }
}

export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<VALUE, MAPPED, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>> {
  return (inputStream, name) => Stream.transformer(new Map(name, inputStream, mapper), inputStream);
}

export namespace map {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, ERROR> = (value: VALUE) => MAPPED | Stream.Error<ERROR>;
}
