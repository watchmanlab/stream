import { Channel, Queue, Source, Stream, Transformer } from "../core/index.ts";

const NAME = "concurrent";

class Concurrent<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = concurrent.Name,
> extends Transformer<INPUT_STREAM, MAPPED, ERROR, NAME> {
  private _options: Required<concurrent.Options>;
  private _buffer: concurrent.BufferEntry<VALUE, MAPPED, ERROR>[] = [];
  private _pending: number = 0;
  private _limitReached?: Stream<number, never, `${NAME}LimitReached`>;
  private _optionsChanged?: Stream<concurrent.OptionsChangedEvent, never, `${NAME}OptionsChanged`>;
  constructor(
    name = NAME as NAME,
    inputStream: INPUT_STREAM,
    mapper: concurrent.Mapper<VALUE, MAPPED, ERROR>,
    options?: concurrent.Options,
  ) {
    super(name, inputStream, () => {
      const output = new Stream<VALUE>();
      const outputChannel = output.channels.get();

      const buffer: Stream.Batch<VALUE>[] = [];

      let counter = 0;

      const input = inputStream.channels.get();

      return {
        next: async () => {
          while (true) {
            const result = await input.next();

            if (result.done) return result;

            counter += result.value.length;

            buffer.push(result.value);

            if (counter >= this._options.limit) break;
          }

          return await outputChannel.next();
        },
        return: async () => {
          //
          return { value: Queue.EMPTY as never, done: true };
        },
      };
    });
    const self = this;
    this._options = {
      ...concurrent.defaultOptions,
      ...Object.fromEntries(Object.entries(options ?? {}).filter(([_, val]) => val != null)),
    };
  }

  override async dispose(): Promise<void> {
    //TODO:
    await super.dispose();
  }
  get options() {
    return { ...this._options };
  }
  set options(options: concurrent.Options) {
    const old = this.options;
    this._options = {
      ...this._options,
      ...Object.fromEntries(Object.entries(options).filter(([_, val]) => val != null)),
    };
    this._optionsChanged?.push({ old, new: options });
  }
  get buffer() {
    return [...this._buffer];
  }
  get pending() {
    return this._pending;
  }
  get limitReached() {
    if (!this._limitReached) this._limitReached = new Stream(`${this.name}LimitReached`);
    return this._limitReached;
  }
  get optionsChanged() {
    if (!this._optionsChanged) this._optionsChanged = new Stream(`${this.name}OptionsChanged`);
    return this._optionsChanged;
  }
}
export function concurrent<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = concurrent.Name,
>(
  mapper: concurrent.Mapper<VALUE, MAPPED, ERROR>,
  options?: concurrent.Options,
): Stream.Transform<INPUT_STREAM, NAME, Concurrent<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>> {
  return (inputStream, name) => new Concurrent(name, inputStream, mapper, options);
}

export namespace concurrent {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, ERROR> = (value: VALUE) => Promise<MAPPED | Source.Error<ERROR>>;
  export type Options = {
    limit?: number;
    ordered?: boolean;
    onDispose?: "drain" | "abort";
  };
  export const defaultOptions: Required<Options> = {
    limit: 1000,
    ordered: false,
    onDispose: "drain",
  };
  export type BufferEntry<VALUE, MAPPED, ERROR> = {
    value: VALUE;
    mapped: MAPPED | Source.Error<ERROR> | Promise<MAPPED | Source.Error<ERROR>>;
  };
  export type OptionsChangedEvent = { old: Options; new: Options };
}
