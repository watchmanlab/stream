import { Stream } from "../../stream";

export class Queue<VALUE, NAME extends string = queue.Name> extends Stream<VALUE, NAME> {
  protected _buffer = new Array<VALUE>();
  protected _events?: Stream<queue.Event<VALUE>, "Event">;
  constructor(source: Stream<VALUE, any>, options?: queue.Options<VALUE, NAME>) {
    const {
      name = NAME as NAME,
      mode = "lazy",
      dropStrategy = "oldest",
      initialValues = [],
      maxSize = 10000,
    } = options ?? {};
    const buffer = [...initialValues];

    let generator: AsyncGenerator<VALUE> | undefined;
    let sourceComplete = false;
    let resolve: (() => void) | undefined;

    if (mode === "eager") startBuffering();

    super(name, async function* () {
      if (mode === "lazy") startBuffering();
      try {
        while (true) {
          if (buffer.length) {
            const value = buffer.pop()!;
            yield value;
            self._events?.push({ type: "consumed", value, size: self.size });
          } else if (sourceComplete) {
            return;
          } else {
            await new Promise<void>((r) => (resolve = r));
          }
        }
      } finally {
        if (mode === "lazy") generator?.return(void 0);
      }
    });

    this._buffer = buffer;

    const self = this;
    let dropped = 0;
    async function startBuffering() {
      generator = source.generator();

      for await (const value of generator) {
        if (buffer.length >= maxSize) {
          if (dropStrategy === "newest") {
            self._events?.push({ type: "evicted", value, dropStrategy, dropped: ++dropped });
            continue;
          }
          self._events?.push({ type: "evicted", value: buffer.pop()!, dropStrategy, dropped: ++dropped });
        }

        buffer.unshift(value);
        self._events?.push({ type: "buffered", value, size: self.size });
        resolve?.();
      }
      sourceComplete = true;
    }
  }

  get events() {
    if (!this._events) this._events = new Stream();
    return this._events;
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
  options?: Omit<queue.Options<VALUE, NAME>, "name">,
): Stream.Transformer<NAME, Stream<VALUE, any>, Queue<VALUE, NAME>> {
  return (_, source, name) => new Queue(source, { ...options, name });
}

export const NAME = "queued";

export namespace queue {
  export type Name = typeof NAME;
  export type Options<VALUE, NAME extends string> = {
    name?: NAME;
    initialValues?: VALUE[];
    maxSize?: number;
    dropStrategy?: Options.DropStrategy;
    mode?: Options.Mode;
  };
  export namespace Options {
    export type DropStrategy = "oldest" | "newest";
    export type Mode = "eager" | "lazy";
  }
  export type Event<VALUE> =
    | { type: "evicted"; value: VALUE; dropStrategy: Options.DropStrategy; dropped: number }
    | { type: "buffered"; value: VALUE; size: number }
    | { type: "consumed"; value: VALUE; size: number };
}

const stream = new Stream<number, "user">().pipe(queue({ mode: "eager" }), "q1");

stream.user.push(1, 2, 3);

stream.listen((v) => console.log(v));
