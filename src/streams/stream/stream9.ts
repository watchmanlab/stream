const NAME = "stream";

export class Stream<VALUE, NAME extends string = Stream.Name> {
  protected listeners: ((value: VALUE) => void)[] = [];

  constructor(public readonly name: NAME = NAME as NAME) {}

  push(...values: VALUE[]) {
    const lenght = this.listeners.length;
    for (let i = 0; i < lenght; i++) {
      const lenght = this.listeners.length;
      const fn = this.listeners[i];
      for (let j = 0; j < lenght; j++) {
        fn(values[j]);
      }
    }
  }

  listen(fn: (value: VALUE) => void, signal?: Stream.AnyStream) {
    const self = this;

    const abortSignal = signal?.listen(abort);

    this.listeners.push(fn);

    return abort;

    function abort() {
      abortSignal?.();
      self.listeners = self.listeners.filter((listener) => listener !== fn);
    }
  }
  pipe<OUTPUT_STREAM extends Stream.AnyStream>(transform: Stream.Transform<this, OUTPUT_STREAM>): OUTPUT_STREAM {
    return transform(this);
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type AnyStream = Stream<any, any>;
  export type Transform<INPUT_STREAM extends AnyStream, OUTPUT_STREAM extends AnyStream> = (
    inputStream: INPUT_STREAM,
  ) => OUTPUT_STREAM;
}
