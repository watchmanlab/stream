import { Stream } from "../../streams/index.ts";

const NAME = "cache";

export class Cache<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  NAME extends string = cache.Name,
> extends Stream<Stream.ExtractValue<INPUT_STREAM>, NAME> {
  private _buffer: CacheEntry<CLEAN_VALUE>[] = [];
  private _options: Required<cache.Options> = { dropStrategy: "oldest", size: 1000, ttl: null };
  private _optionsChanged?: Stream<Partial<cache.Options>, `${NAME}OptionsChanged`>;
  private _evicted?: Stream<{ value: CLEAN_VALUE; reason: "size" | "ttl" }, `${NAME}Evicted`>;
  private _buffered?: Stream<CLEAN_VALUE, `${NAME}Buffered`>;
  private _cleanupStarted?: Stream<void, `${NAME}CleanupStarted`>;
  private _cleanupStoped?: Stream<void, `${NAME}CleanupStoped`>;
  private _cleared?: Stream<void, `${NAME}Cleared`>;

  constructor(name: NAME, inputStream: INPUT_STREAM, options?: cache.Options) {
    super(name, async function* () {
      try {
        for await (const value of inputStream) {
          yield value;

          if (Stream.isSentinel(value)) continue;

          if (self._options.size <= 0) continue;

          if (self._buffer.length >= self._options.size) {
            if (self._options.dropStrategy === "newest") {
              self._evicted?.push({ value, reason: "size" });
              continue;
            } else {
              const value = self._buffer.shift()!.value;
              self._evicted?.push({ value, reason: "size" });
            }
          }

          self._buffer.push({ value, timestamp: Date.now() });
          self.startCleanup();
          self._buffered?.push(value);
        }
      } finally {
      }
    });

    const self = Stream.traversable(this, inputStream);

    this._options = { ...this._options, ...options };
  }

  private cleanupTimer: any;
  private startCleanup() {
    if (this.cleanupTimer || !this._buffer.length) return;

    const ttl = this._options.ttl;

    if (!ttl) {
      this.stopCleanup();
      return;
    }

    this._cleanupStarted?.push();
    this.cleanupTimer = setInterval(
      () => {
        const now = Date.now();
        let i = 0;

        while (i < this._buffer.length) {
          if (now - this._buffer[i].timestamp >= ttl) {
            const [entry] = this._buffer.splice(i, 1);
            this._evicted?.push({ value: entry.value, reason: "ttl" });
          } else {
            i++;
          }
        }

        if (this._buffer.length === 0) this.stopCleanup();
      },
      Math.min(ttl / 4, Math.max(500, ttl / 10)),
    );
  }
  private stopCleanup() {
    if (this.cleanupTimer !== undefined) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = undefined;
      this._cleanupStoped?.push();
    }
  }
  get options() {
    return { ...this._options };
  }
  set options(options: cache.Options) {
    this._options = { ...this._options, ...options };

    if (options.size && this._options.size > options.size) {
      const count = (this._options.size = options.size);
      if (this._options.dropStrategy === "newest") {
        this._buffer.splice(this._buffer.length - 1 - count, count);
      } else {
        this._buffer.splice(0, count);
      }
    }
    if (options.ttl) this.startCleanup();

    if (Object.keys(options).length) this._optionsChanged?.push(options);
  }
  get values() {
    return this._buffer.map((v) => v.value);
  }
  get evicted() {
    if (!this._evicted) this._evicted = new Stream(`${this._name}Evicted`);
    return this._evicted;
  }
  get buffered() {
    if (!this._buffered) this._buffered = new Stream(`${this._name}Buffered`);
    return this._buffered;
  }
  get optionsChanged() {
    if (!this._optionsChanged) this._optionsChanged = new Stream(`${this._name}OptionsChanged`);
    return this._optionsChanged;
  }
  get cleanupStarted() {
    if (!this._cleanupStarted) this._cleanupStarted = new Stream(`${this._name}CleanupStarted`);
    return this._cleanupStarted;
  }
  get cleanupStoped() {
    if (!this._cleanupStoped) this._cleanupStoped = new Stream(`${this._name}CleanupStoped`);
    return this._cleanupStoped;
  }
  get cleared() {
    if (!this._cleared) this._cleared = new Stream(`${this._name}Cleared`);
    return this._cleared;
  }
  clear() {
    const length = this._buffer.length;
    this.stopCleanup();
    this._buffer.length = 0;
    if (length) this._cleared?.push();
  }
}
export function cache<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  NAME extends string = cache.Name,
>(
  options: cache.Options,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Cache<INPUT_STREAM, CLEAN_VALUE, NAME>, INPUT_STREAM>>;
export function cache<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  NAME extends string = cache.Name,
>(): Stream.Transform<INPUT_STREAM, Stream.Traversable<Cache<INPUT_STREAM, CLEAN_VALUE, NAME>, INPUT_STREAM>>;
export function cache<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
>(name: NAME): Stream.Transform<INPUT_STREAM, Stream.Traversable<Cache<INPUT_STREAM, CLEAN_VALUE, NAME>, INPUT_STREAM>>;
export function cache<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
>(
  name: NAME,
  options: cache.Options,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Cache<INPUT_STREAM, CLEAN_VALUE, NAME>, INPUT_STREAM>>;
export function cache<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  NAME extends string = cache.Name,
>(
  nameOrOptions?: NAME | cache.Options,
  options?: cache.Options,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Cache<INPUT_STREAM, CLEAN_VALUE, NAME>, INPUT_STREAM>> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrOptions === "string"
        ? new Cache(nameOrOptions, inputStream, options)
        : new Cache(NAME as NAME, inputStream, nameOrOptions),
      inputStream,
    );
}

export namespace cache {
  export type Name = typeof NAME;
  export type Options = { size?: number; dropStrategy?: "oldest" | "newest"; ttl?: number | null };
}
type CacheEntry<CLEAN_VALUE> = {
  value: CLEAN_VALUE;
  timestamp: number;
};
