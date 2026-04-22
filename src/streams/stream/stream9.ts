import { Stream as OldStream } from "./stream";
export class Stream<VALUE> implements Iterable<Stream.Consumer<VALUE>>, AsyncIterable<VALUE>, Disposable {
  private consumers: Stream.Consumer<VALUE>[] = [];

  constructor() {}

  [Symbol.dispose]() {
    this.consumers.length = 0;
  }
  [Symbol.iterator]() {
    return this.consumers[Symbol.iterator]();
  }
  async *[Symbol.asyncIterator]() {
    let resolve: (value: VALUE) => void;
    let promise = new Promise<VALUE>((r) => (resolve = r));

    const { abort, ready } = this.listen((value) => {
      resolve?.(value);
    });

    try {
      while (true) {
        const value = await promise;
        promise = new Promise<VALUE>((r) => (resolve = r));
        ready();
        yield value;
      }
    } finally {
      abort();
    }
  }

  push(value: VALUE) {
    const consumers = this.consumers;
    const length = consumers.length;

    for (let i = 0; i < length; i++) {
      const consumer = consumers[i];
      if (consumer.isReady) {
        consumer.isReady = false;
        consumer.fn(value, consumer);
      } else {
        consumer.buffer.push(value);
      }
    }
  }

  listen(fn: Stream.Fn<VALUE>): Stream.Consumer<VALUE> {
    const buffer: VALUE[] = [];
    const consumer: Stream.Consumer<VALUE> = { ready, buffer, isReady: true, abort, fn };
    const consumers = this.consumers;

    consumers.push(consumer);

    return consumer;
    function ready() {
      consumer.isReady = true;
      const value = buffer.shift();
      if (!value) return;

      consumer.isReady = false;
      fn(value, consumer);
    }
    function abort() {
      buffer.length = 0;

      const index = consumers.indexOf(consumer);
      if (index === -1) return;

      consumers.splice(index, 1);
    }
  }
}

export namespace Stream {
  export type Abort = () => void;
  export type Next = () => void;
  export type Fn<VALUE> = (value: VALUE, consumer: Consumer<VALUE>) => void;
  export type Consumer<VALUE> = { ready: Next; buffer: VALUE[]; isReady: boolean; abort: Abort; fn: Fn<VALUE> };
}

function simpleTest() {
  const stream = new Stream<number>();
  stream.listen(async (value, { ready, abort }) => {
    await new Promise((r) => setTimeout(r, Math.random() * 200));
    ready();

    console.log("listener", value);
  });

  // (async () => {
  //   for await (const value of stream) {
  //     console.log("generator", value);
  //   }

  //   console.log("abort");
  // })();

  (async () => {
    stream.push(1);
    stream.push(2);
    stream.push(3);
  })();
}
function newStreamBench() {
  const MAX = 1_000_000;
  const now = performance.now();

  const stream = new Stream<number>();
  stream.listen((value, { ready, abort }) => {
    // await new Promise((r) => setTimeout(r, Math.random() * 200));
    if (value === MAX) {
      console.log("new stream", Math.round(performance.now() - now));
      abort();
      return;
    }
    ready();
  });
  // (async () => {
  //   for await (const value of stream) {
  //     if (value === MAX) {
  //       console.log("new stream gen", Math.round(performance.now() - now));

  //       return;
  //     }
  //   }
  // })();

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}
function oldStreamBench() {
  const MAX = 1_000_000;

  const stream = new OldStream<number>();

  stream.listen((value) => {
    // await new Promise((r) => setTimeout(r, Math.random() * 200));
    if (value === MAX) console.log("old stream", Math.round(performance.now() - now));
  });

  const now = performance.now();
  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}

simpleTest();
// newStreamBench();
// oldStreamBench()
