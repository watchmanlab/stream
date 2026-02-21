import { Stream } from "../../streams";

const NAME = "queued";

class Queue<VALUE, NAME extends string = queue.Name> extends Stream<VALUE, NAME> {
  protected _buffer = new Array<VALUE>();
  protected _events?: Stream<queue.Event<VALUE, NAME>, `${NAME}-events`>;
  protected _options: Required<queue.Options> = { dropStrategy: "oldest", maxSize: 10000 };
  protected _dropped = 0;
  protected _resolvers = new Set<() => void>();

  constructor(source: Stream<VALUE, any>, name = NAME as NAME, options?: queue.Options) {
    super(name, async function* () {
      let resolve;

      try {
        while (true) {
          if (self._buffer.length) {
            const value = self._buffer.pop()!;
            yield value;
            self._events?.push({ type: "consumed", value, self });
          } else {
            await new Promise<void>((res) => {
              resolve = res;
              self._resolvers.add(resolve);
            });
          }
        }
      } finally {
        self._resolvers.delete(resolve!);
        resolve!();
      }
    });

    const self = this;
    this.options = options ?? {};

    (async () => {
      for await (const value of source) {
        if (self._buffer.length >= self._options.maxSize) {
          self._dropped++;
          if (self._options.dropStrategy === "newest") {
            self._events?.push({ type: "evicted", value, self });
            continue;
          } else {
            self._events?.push({ type: "evicted", value: self._buffer.pop()!, self });
          }
        }

        self._buffer.unshift(value);
        self._events?.push({ type: "buffered", value, self });
        self._resolvers.forEach((resolver) => resolver());
      }
    })();
  }

  // get events() {
  //   if (!this._events) this._events = new Stream();
  //   return this._events;
  // }
  get options() {
    return { ...this._options };
  }
  set options(options: queue.Options) {
    this._options = { ...this._options, ...options };
  }
  get dropped() {
    return this._dropped;
  }
  get size() {
    return this._buffer.length;
  }
  get values() {
    return [...this._buffer];
  }
  clear() {
    this._buffer.length = 0;
  }
}

export function queue<VALUE, NAME extends string = queue.Name>(
  options?: queue.Options,
): Stream.Transformer<NAME, Stream<VALUE, any>, Queue<VALUE, NAME>> {
  return (_, source, name) => new Queue(source, name, options);
}

export namespace queue {
  export type Name = typeof NAME;
  export type Options = {
    maxSize?: number;
    dropStrategy?: "oldest" | "newest";
  };

  export type Event<VALUE, NAME extends string> =
    | { type: "evicted"; value: VALUE; self: Queue<VALUE, NAME> }
    | { type: "buffered"; value: VALUE; self: Queue<VALUE, NAME> }
    | { type: "consumed"; value: VALUE; self: Queue<VALUE, NAME> };
}
