import { Stream } from "../../stream";

export function cache<VALUE>(
  options?: cache.Options<VALUE>,
): Stream.Transformer<Stream<VALUE>, Stream<VALUE> & { cache: cache.Cache<VALUE> }> {
  return function (source) {
    const { initialValues = [], size = 1000, dropStrategy = "oldest", ttl } = options ?? {};

    const cacheArray: CacheEntry<VALUE>[] = initialValues.map((value) => ({
      value,
      timestamp: Date.now(),
    }));

    const output = new Stream<VALUE>();
    const evicted = new Stream<{ value: VALUE; reason: "size" | "ttl" }>();

    let cleanupTimer: any;

    const startCleanup = () => {
      if (ttl === undefined || cleanupTimer !== undefined) return;

      cleanupTimer = setInterval(
        () => {
          const now = Date.now();
          let i = 0;
          while (i < cacheArray.length) {
            if (now - cacheArray[i].timestamp >= ttl) {
              const [entry] = cacheArray.splice(i, 1);
              evicted.push({ value: entry.value, reason: "ttl" });
            } else {
              i++;
            }
          }

          // Stop timer if cache is empty
          if (cacheArray.length === 0 && cleanupTimer !== undefined) {
            clearInterval(cleanupTimer);
            cleanupTimer = undefined;
          }
        },
        Math.min(ttl / 4, Math.max(500, ttl / 10)),
      ); // Check at quarter TTL, min 500ms, max 10% of TTL
    };

    const stopCleanup = () => {
      if (cleanupTimer !== undefined) {
        clearInterval(cleanupTimer);
        cleanupTimer = undefined;
      }
    };

    // Start cleanup if we have initial values with TTL
    if (ttl !== undefined && cacheArray.length > 0) {
      startCleanup();
    }

    // HOT: Start listening immediately to cache ALL events
    source.listen((value) => {
      const entry: CacheEntry<VALUE> = {
        value,
        timestamp: Date.now(),
      };

      cacheArray.push(entry);
      startCleanup();

      if (cacheArray.length > size) {
        const entry = dropStrategy === "oldest" ? cacheArray.shift() : cacheArray.pop();
        if (entry) evicted.push({ value: entry.value, reason: "size" });
      }

      output.push(value);
    });

    Object.defineProperty(output, "cache", {
      value: {
        get values() {
          return cacheArray.map((entry) => entry.value);
        },
        get size() {
          return size;
        },
        get dropStrategy() {
          return dropStrategy;
        },
        get ttl() {
          return ttl;
        },
        get evicted() {
          return evicted;
        },
        clear() {
          stopCleanup();
          cacheArray.length = 0;
        },
      },
      enumerable: true,
      configurable: false,
    });

    return output as Stream<VALUE> & { cache: cache.Cache<VALUE> };
  };
}

export namespace cache {
  export type Cache<VALUE> = {
    readonly values: VALUE[];
    readonly size: number;
    readonly dropStrategy: "oldest" | "newest";
    readonly ttl: number | undefined;
    readonly evicted: Stream<{ value: VALUE; reason: "size" | "ttl" }>;
    clear(): void;
  };

  export type Options<VALUE> = {
    initialValues?: VALUE[];
    size?: number;
    dropStrategy?: "oldest" | "newest";
    ttl?: number;
  };
}
type CacheEntry<VALUE> = {
  value: VALUE;
  timestamp: number;
};
