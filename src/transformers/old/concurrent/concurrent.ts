//@ts-nocheck

import { Stream } from "../../../streams/index.ts";

const NAME = "concurrent";

class Concurrent<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = concurrent.Name,
> extends Stream<
  | MAPPED
  | Stream.ExtractSentinel<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<Concurrent<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  protected _options: Required<concurrent.Options> = {
    concurrencyLimit: 1000,
    preserveOrder: false,
    onTerminate: "drain",
  };
  protected _buffer: (
    | {
        value: CLEAN_VALUE;
        mapped:
          | MAPPED
          | Stream.Error<ERROR>
          | Stream.Terminate
          | Stream.Skip
          | Promise<MAPPED | Stream.Error<ERROR> | Stream.Terminate | Stream.Skip>;
      }
    | Stream.Sentinel
  )[] = [];
  protected _pending: number = 0;
  protected _concurrencyLimitReached?: Stream<number, `${NAME}ConcurrencyLimitReached`>;
  constructor(
    name: NAME,
    inputStream: INPUT_STREAM,
    mapper: concurrent.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
    options?: concurrent.Options,
  ) {
    super(name, async function* () {
      let resolver: () => void;
      let concurrencyLimitResolver: () => void;
      let aborted = false;
      const generator = Stream.generator(inputStream);

      (async () => {
        for await (const value of generator) {
          if (aborted) break;
          if (self._pending >= self._options.concurrencyLimit) {
            self._concurrencyLimitReached?.push(self._options.concurrencyLimit);
            await new Promise<void>((r) => (concurrencyLimitResolver = r));
          }
          self._pending++;

          if (Stream.isSentinel(value)) {
            self._buffer.push(value);
            continue;
          }

          if (self._options.preserveOrder) {
            try {
              self._buffer.push({ value, mapped: mapper(value) });
              resolver!?.();
            } catch (error: any) {
              self._buffer.push({ value, mapped: Stream.genericError<ERROR>(error) });
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
                self._buffer.push({ value, mapped: Stream.genericError<ERROR>(error) });
              });
          }
        }
        if (self._options.onTerminate === "abort") aborted = true;
        resolver!?.();
      })();
      try {
        while (true) {
          if (self._buffer.length && !aborted) {
            const entry = self._buffer.shift()!;
            if (Stream.isSentinel(entry)) {
              yield entry as never;
              continue;
            }

            if (entry.mapped instanceof Promise) self._pending--;

            try {
              const mapped = entry.mapped instanceof Promise ? await entry.mapped : entry.mapped;

              if (Stream.isGenericError<ERROR>(mapped)) {
                yield Stream.sourceErr({
                  value: entry.value,
                  error: mapped.value,
                  source: self,
                }) as never;
                continue;
              }

              yield mapped as never;

              concurrencyLimitResolver!?.();
            } catch (error) {
              yield Stream.sourceErr({
                value: entry.value,
                error,
                source: self,
              }) as never;
            }
          } else {
            if (!aborted) await new Promise<void>((r) => (resolver = r));
          }
        }
      } finally {
        self._buffer.length = 0;
        aborted = true;
        concurrencyLimitResolver!?.();
        resolver!?.();
        await generator.return();
      }
    });
    const self = Stream.traversable(this, inputStream);
    this.options = options ?? {};
  }

  get options() {
    return this._options;
  }
  set options(options: concurrent.Options) {
    this._options = { ...this._options, ...options };
  }
  get concurrencyLimitReached() {
    if (!this._concurrencyLimitReached)
      this._concurrencyLimitReached = new Stream(`${this._name}ConcurrencyLimitReached`);
    return this._concurrencyLimitReached;
  }
  get buffer() {
    return [...this._buffer];
  }
  get pending() {
    return this._pending;
  }
}
export function concurrent<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
>(
  mapper: concurrent.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
  options?: concurrent.Options,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Concurrent<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, concurrent.Name>, INPUT_STREAM>
>;
export function concurrent<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
>(
  name: NAME,
  mapper: concurrent.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
  options?: concurrent.Options,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Concurrent<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
>;
export function concurrent<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = concurrent.Name,
>(
  nameOrMapper: NAME | concurrent.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
  mapperOrOptions?: concurrent.Mapper<CLEAN_VALUE, MAPPED, ERROR> | concurrent.Options,
  options?: concurrent.Options,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Concurrent<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrMapper === "string"
        ? new Concurrent(
            nameOrMapper,
            inputStream,
            mapperOrOptions as concurrent.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
            options,
          )
        : new Concurrent(NAME as NAME, inputStream, nameOrMapper, mapperOrOptions as concurrent.Options),
      inputStream,
    );
}

export namespace concurrent {
  export type Name = typeof NAME;
  export type Mapper<CLEAN_VALUE, MAPPED, ERROR> = (
    value: CLEAN_VALUE,
  ) => Promise<MAPPED | Stream.Error<ERROR> | Stream.Terminate | Stream.Skip>;
  export type Options = {
    concurrencyLimit?: number;
    preserveOrder?: boolean;
    onTerminate?: "drain" | "abort";
  };
}
