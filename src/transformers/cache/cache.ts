import { Stream } from "../../streams/index.ts";

const NAME = "Cache";

export class Cache<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  NAME extends string = cache.Name,
> extends Stream<Stream.ExtractValue<SOURCE>, NAME> {
  protected _buffer: CacheEntry<CLEAN_VALUE>[] = [];
  protected _options: Required<cache.Options> = { dropStrategy: "oldest", size: 1000, ttl: null };
  protected _events?: Stream<cache.Event<CLEAN_VALUE, this>>;
  protected _dropped = 0;

  constructor(source: SOURCE, name = NAME as NAME, options?: cache.Options) {
    super(name, source);

    this.options = options ?? {};

    (async () => {
      for await (const value of source) {
        if (Stream.isSentinel(value)) continue;
        if (this._buffer.length >= this._options.size) {
          this._dropped++;
          if (this._options.dropStrategy === "newest") {
            this._events?.push({ type: "evicted", value, reason: "size", self: this });
            continue;
          } else {
            this._events?.push({ type: "evicted", value: this._buffer.pop()!.value, reason: "size", self: this });
          }
        }

        this._buffer.unshift(value);
        this._events?.push({ type: "buffered", value, self: this });

        this._buffer.push({ value, timestamp: Date.now() });
        this.startCleanup();
      }
    })();
  }

  protected cleanupTimer: any;
  protected startCleanup() {
    const ttl = this._options.ttl;

    if (!ttl || this.cleanupTimer !== undefined) return;

    this.cleanupTimer = setInterval(
      () => {
        const now = Date.now();
        let i = 0;
        while (i < this._buffer.length) {
          if (now - this._buffer[i].timestamp >= ttl) {
            const [entry] = this._buffer.splice(i, 1);
            this._events?.push({ type: "evicted", value: entry.value, reason: "ttl", self: this });
          } else {
            i++;
          }
        }

        // Stop timer if cache is empty
        if (this._buffer.length === 0 && this.cleanupTimer !== undefined) {
          clearInterval(this.cleanupTimer);
          this.cleanupTimer = undefined;
        }
      },
      Math.min(ttl / 4, Math.max(500, ttl / 10)),
    ); // Check at quarter TTL, min 500ms, max 10% of TTL
  }
  protected stopCleanup() {
    if (this.cleanupTimer !== undefined) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = undefined;
    }
  }
  get options() {
    return { ...this._options };
  }
  set options(options: cache.Options) {
    this._options = { ...this._options, ...options };
  }
  get values() {
    return this._buffer.map((v) => v.value);
  }

  clear() {
    this.stopCleanup();
    this._buffer.length = 0;
  }
}
export function cache<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  NAME extends string = cache.Name,
>(options?: cache.Options): Stream.Transform<NAME, SOURCE, Cache<SOURCE, CLEAN_VALUE, NAME>> {
  return (_, source, name) => new Cache(source, name, options);
}

export namespace cache {
  export type Name = typeof NAME;
  export type Options = { size?: number; dropStrategy?: "oldest" | "newest"; ttl?: number | null };
  export type Event<CLEAN_VALUE, SELF extends Stream<any, any>> =
    | {
        type: "evicted";
        value: CLEAN_VALUE;
        reason: "size" | "ttl";
        self: SELF;
      }
    | {
        type: "buffered";
        value: CLEAN_VALUE;
        self: SELF;
      };
}
type CacheEntry<CLEAN_VALUE> = {
  value: CLEAN_VALUE;
  timestamp: number;
};
