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

    const { done, next } = this.listen((value) => {
      resolve?.(value);
    });

    try {
      while (true) {
        const value = await promise;
        promise = new Promise<VALUE>((r) => (resolve = r));
        next();
        yield value;
      }
    } finally {
      done();
    }
  }

  push(value: VALUE) {
    const consumers = this.consumers;
    const length = consumers.length;

    for (let i = 0; i < length; i++) {
      const consumer = consumers[i];
      if (!consumer.processing) {
        consumer.processing = true;
        consumer.fn(value, consumer.next, consumer.done);
      } else {
        consumer.buffer.push(value);
      }
    }
  }

  listen(fn: (value: VALUE, next: () => void, done: () => void) => void) {
    const buffer: VALUE[] = [];
    const consumer: Stream.Consumer<VALUE> = { next, buffer, processing: false, done, fn };
    const consumers = this.consumers;

    consumers.push(consumer);

    return { next, done };
    function next() {
      consumer.processing = false;
      const value = buffer.shift();
      if (!value) return;

      consumer.processing = true;
      fn(value, next, done);
    }
    function done() {
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
  export type Fn<VALUE> = (value: VALUE, next: Next, done: Abort) => void;
  export type Consumer<VALUE> = { next: Next; buffer: VALUE[]; processing: boolean; done: Abort; fn: Fn<VALUE> };
}

function simpleTest() {
  const stream = new Stream<number>();
  // stream.listen(async (value, next, done) => {
  //   // next();
  //   await new Promise((r) => setTimeout(r, Math.random() * 200));

  //   console.log("listener", value, [...stream][0]);
  // });

  (async () => {
    for await (const value of stream) {
      console.log("generator", value);
    }

    console.log("done");
  })();

  (async () => {
    stream.push(1);
    stream.push(2);
    stream.push(3);
  })();
}
function newStreamBench() {
  const MAX = 1_000_000;

  const stream = new Stream<number>();
  stream.listen((value, next, done) => {
    // await new Promise((r) => setTimeout(r, Math.random() * 200));
    if (value === MAX) {
      console.log("new stream", Math.round(performance.now() - now));
      done();
      return;
    }
    next();
  });

  const now = performance.now();
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
// newStreamBench()
// oldStreamBench()
