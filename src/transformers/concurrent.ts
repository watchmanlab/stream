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
      const output = new Stream<MAPPED>().getChannel();
      const request = new Stream<void>();
      const batch: MAPPED[] = [];

      let resolver: () => void;
      let limitResolver: () => void;
      let aborted = false;
      const channel = inputStream.getChannel();

      return {
        next: async () => {
          return await output.next();
        },
        return: async () => {
          //
          return { value: Queue.EMPTY as never, done: true };
        },
      };

      (async () => {
        for await (const batch of channel) {
          for (let i = 0, length = batch.length; i < length; i++) {
            const value = batch[i];

            if (aborted) break;
            if (self._pending >= self._options.limit) {
              self._limitReached?.push(self._options.limit);
              await new Promise<void>((r) => (limitResolver = r));
            }
            self._pending++;

            if (self._options.ordered) {
              try {
                self._buffer.push({ value, mapped: mapper(value) });
                resolver!?.();
              } catch (error: any) {
                self._buffer.push({ value, mapped: new Source.Error(error) });
              }
            } else {
              mapper(value)
                .then((mapped) => {
                  self._pending--;
                  if (aborted) return;
                  self._buffer.push({ value, mapped });
                  resolver!?.();
                })
                .catch((error) => {
                  self._buffer.push({ value, mapped: new Source.Error(error) });
                });
            }
          }
        }
        if (self._options.onDispose === "abort") aborted = true;
        resolver!?.();
      })();
      try {
        //TODO:: i think we will use stream as output so the produced simply push to it
        while (true) {
          if (self._buffer.length && !aborted) {
            const entry = self._buffer.shift()!;

            if (entry.mapped instanceof Promise) self._pending--;

            try {
              const mapped = entry.mapped instanceof Promise ? await entry.mapped : entry.mapped;

              if (mapped instanceof Source.Error) {
                self.source?.throw(mapped.data);
                continue;
              }

              yield[mapped];

              limitResolver!?.();
            } catch (error: any) {
              self.source?.throw(error);
            }
          } else {
            if (!aborted) await new Promise<void>((r) => (resolver = r));
          }
        }
      } finally {
        self._buffer.length = 0;
        aborted = true;
        limitResolver!?.();
        resolver!?.();
        await channel.return();
      }
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
