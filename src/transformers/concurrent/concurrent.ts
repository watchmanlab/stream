import { Stream } from "../../streams";

const NAME = "concurrent";

export class Concurrent<VALUE, MAPPED, NAME extends string = concurrent.Name> extends Stream<MAPPED, NAME> {
  protected _options: Required<concurrent.Options>;
  protected _buffer: (MAPPED | Promise<MAPPED>)[] = [];
  protected _pending: number = 0;
  protected _events?: Stream<concurrent.Event<VALUE, MAPPED, NAME>, `${NAME}-events`>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    mapper: concurrent.Mapper<VALUE, MAPPED>,
    options?: concurrent.Options,
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
            const result = self._buffer.shift()!;

            if (result instanceof Promise) self._pending--;

            yield await result;

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
  set options(options: concurrent.Options) {
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

export function concurrent<VALUE, MAPPED, NAME extends string = concurrent.Name>(
  mapper: concurrent.Mapper<VALUE, MAPPED>,
  options?: concurrent.Options,
): Stream.Transformer<NAME, Stream<VALUE, any>, Concurrent<VALUE, MAPPED, NAME>> {
  return (_, source, name) => new Concurrent(source, name, mapper, options);
}

export namespace concurrent {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => Promise<MAPPED>;
  export type Options = {
    concurrencyLimit?: number;
    preserveOrder?: boolean;
  };
  export type Event<VALUE, MAPPED, NAME extends string = concurrent.Name> = {
    type: "concurrency-limit-reached";
    self: Concurrent<VALUE, MAPPED, NAME>;
  };
}
