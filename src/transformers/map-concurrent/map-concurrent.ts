import { Stream } from "../../streams";
import { consumer } from "../consumer";

const NAME = "mapConcurrent";

export class MapConcurrent<VALUE, MAPPED, NAME extends string = mapConcurrent.Name> extends Stream<MAPPED, NAME> {
  protected _options: Required<mapConcurrent.Options>;
  protected _buffer: (MAPPED | Promise<MAPPED>)[] = [];
  protected _pending: number = 0;
  protected _events?: Stream<mapConcurrent.Event<VALUE, MAPPED, NAME>, `${NAME}-events`>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    mapper: mapConcurrent.Mapper<VALUE, MAPPED>,
    options?: mapConcurrent.Options,
  ) {
    super(name, async function* () {
      let resolver: () => void;
      let concurrencyLimitResolver: () => void;
      let aborted = false;
      const generator = Stream.generator(source);

      (async () => {
        for await (const value of generator) {
          if (aborted) break;
          if (self._pending >= self._options.concurrencyLimit) {
            self._events?.push({ type: "concurrency-limit-reached", self });
            await new Promise<void>((r) => (concurrencyLimitResolver = r));
          }
          self._pending++;

          if (self._options.preserveOrder) {
            self._buffer.push(mapper(value));
            resolver!?.();
          } else {
            mapper(value).then((mapped) => {
              self._pending--;
              if (aborted) return;
              self._buffer.push(mapped);
              resolver!?.();
            });
          }
        }
        aborted = true;
        resolver!?.();
      })();
      try {
        while (true) {
          if (self._buffer.length && !aborted) {
            yield await self._buffer.shift()!;

            if (self._options.preserveOrder) self._pending--;

            concurrencyLimitResolver!?.();
          } else {
            await new Promise<void>((r) => (resolver = r));
          }
        }
      } finally {
        self._buffer.length = 0;
        aborted = true;
        generator.return();
        concurrencyLimitResolver!?.();
        resolver!?.();
      }
    });
    const self = this;
    this._options = { concurrencyLimit: 1000, preserveOrder: false, ...options };
  }

  get options() {
    return this._options;
  }
  set options(options: mapConcurrent.Options) {
    this._options = { ...this._options, ...options };
  }
  get events() {
    if (!this._events) this._events = new Stream(`${this._name}-events` as never);
    return this._events;
  }
  get buffer() {
    return [...this._buffer];
  }
  get pending() {
    return this._pending;
  }
}

export function mapConcurrent<VALUE, MAPPED, NAME extends string = mapConcurrent.Name>(
  mapper: mapConcurrent.Mapper<VALUE, MAPPED>,
  options?: mapConcurrent.Options,
): Stream.Transformer<NAME, Stream<VALUE, any>, MapConcurrent<VALUE, MAPPED, NAME>> {
  return (_, source, name) => new MapConcurrent(source, name, mapper, options);
}

export namespace mapConcurrent {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => Promise<MAPPED>;
  export type Options = {
    concurrencyLimit?: number;
    preserveOrder?: boolean;
  };
  export type Event<VALUE, MAPPED, NAME extends string = mapConcurrent.Name> = {
    type: "concurrency-limit-reached";
    self: MapConcurrent<VALUE, MAPPED, NAME>;
  };
}

const s = new Stream<number>()
  .pipe(
    mapConcurrent(
      async (v) => {
        await new Promise((r) => setTimeout(r, Math.random() * 500));
        return v.toFixed(3);
      },
      { preserveOrder: true, concurrencyLimit: 1 },
    ),
  )
  .pipe(consumer((v) => console.log(v)));

s.mapConcurrent.root.push(1, 2, 3, 4, 5, 6, 7, 8, 9);
