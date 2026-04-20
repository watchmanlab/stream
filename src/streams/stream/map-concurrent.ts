import { Stream } from "./stream";

const NAME = "mapConcurrent";

class MapConcurrent<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = mapConcurrent.Name,
> extends Stream<MAPPED, NAME> {
  private _error?: Stream<{ value: VALUE; error: ERROR }, `${NAME}Error`>;

  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: mapConcurrent.Mapper<VALUE, MAPPED, ERROR>) {
    super(name);

    let abort: Stream.Abort;
    this.firstListenerAdded.listen(() => {
      abort = inputStream.listen(async (value) => {
        try {
          const result = await mapper(value);

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
      });
    });
    this.lastListenerRemoved.listen(() => {
      abort();
    });
    this.terminated.listen(() => {
      abort();
    });
  }

  get error() {
    if (!this._error) this._error = new Stream(`${this.name}Error`);
    return this._error;
  }
}

export function mapConcurrent<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = mapConcurrent.Name,
>(
  mapper: mapConcurrent.Mapper<VALUE, MAPPED, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, MapConcurrent<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>> {
  return (inputStream, name) => Stream.transformer(new MapConcurrent(name, inputStream, mapper), inputStream);
}

export namespace mapConcurrent {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, ERROR> = (value: VALUE) => Promise<MAPPED | Stream.Error<ERROR>>;
}
