import { Stream } from "../../stream";

export function queue<VALUE>(options?: queue.Options<VALUE>): Stream.Transformer<Stream<VALUE>, Stream<VALUE>> {
  const { size = 1000, dropStrategy = "oldest" } = options ?? {};
  return (source) => {
    const buffer: VALUE[] = [];
    const evicted = new Stream<VALUE>();
    let consuming = false;

    source.listen((value) => {
      buffer.push(value);
      if (buffer.length > size) {
        const valueDropped = dropStrategy === "oldest" ? buffer.shift() : buffer.pop();
        if (valueDropped) evicted.push(valueDropped);
      }
    });

    const output = new Stream<VALUE>((self) => {
      consuming = true;
      while (buffer.length) {
        self.push(buffer.shift()!);
      }

      return new Stream.Controller(() => (consuming = false));
    });

    return output;
  };
}

export namespace queue {
  export type Options<VALUE> = {
    initialValues?: VALUE[];
    size?: number;
    dropStrategy?: "oldest" | "newest";
  };

  export type Queue<VALUE> = {
    readonly values: VALUE[];
    readonly size: number;
    readonly evicted: Stream<VALUE>;
    clear(): void;
  };
}
