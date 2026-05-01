import { Stream } from "../../stream/stream";

const NAME = "mapConcurrent";

class MapConcurrent<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = mapConcurrent.Name,
> extends Stream<MAPPED, NAME> {
  private _options: Required<mapConcurrent.Options>;
  private _error?: Stream<{ value: VALUE; error: ERROR }, `${NAME}Error`>;
  private _buffer: VALUE[] = [];
  private _queue: (MAPPED | Promise<MAPPED>)[] = [];
  private _resolve: Function = Function;
  private _concurrencyLimitReached?: Stream<void, `${NAME}ConcurrencyLimitReached`>;
  constructor(
    name = NAME as NAME,
    inputStream: INPUT_STREAM,
    mapper: mapConcurrent.Mapper<VALUE, MAPPED, ERROR>,
    options = mapConcurrent.defaultOptions,
  ) {
    super(name);

    this._options = options;

    const abort = inputStream.listen((value) => {
      if (!this.listenersCount) return;
      if (!this._options.preserveOrder) this._buffer.push(value);
      this._resolve();
    });

    inputStream.listen(async (value) => {
      if (!this.listenersCount) return;
      try {
        const result = await mapper(value);

        if (result instanceof Stream.Error) {
          const sourceError = new Stream.SourceError(value, result.value, this);
          if (!this._error?.listenersCount) throw sourceError;
          this._error?.push(sourceError);
        } else {
          this.push(result);
        }
      } catch (error: any) {
        if (error instanceof Stream.SourceError) {
          if (!this._error?.listenersCount) throw error;
          this._error?.push(error);
        } else {
          const sourceError = new Stream.SourceError(value, error, this);
          if (!this._error?.listenersCount) throw sourceError;
          this._error?.push(sourceError);
        }
      }
    });

    this.terminated.listenOnce(abort);
  }
  private async run() {
    while (true) {}
  }
  get options() {
    return { ...this._options };
  }
  set options(options: mapConcurrent.Options) {
    this._options = {
      ...this._options,
      ...Object.fromEntries(Object.entries(options).filter(([_, val]) => val !== undefined)),
    };
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
  export type Options = {
    concurrencyLimit?: number;
    preserveOrder?: boolean;
    onTerminate?: "drain" | "abort";
  };
  export const defaultOptions: Required<Options> = {
    concurrencyLimit: Infinity,
    preserveOrder: false,
    onTerminate: "drain",
  };
}
