import { Stream as OldStream } from "./stream";
export class Stream<VALUE> {
  private consumers: Stream.Consumer<VALUE>[] = [];

  constructor() {}

  push(value: VALUE) {
    const consumers = this.consumers;
    const length = consumers.length;

    for (let i = 0; i < length; i++) {
      const consumer = consumers[i];
      const length = consumer.buffer.length;
      if (length === 0 && !consumer.processing) {
        consumer.processing = true;
        consumer.fn(value, consumer.next);
      } else {
        consumers.push(consumer);
      }
    }
  }

  listen(fn: (value: VALUE, next: () => void) => void) {
    const buffer: VALUE[] = [];
    const consumer: Stream.Consumer<VALUE> = { next, buffer, processing: false, fn };
    const consumers = this.consumers;

    consumers.push(consumer);

    return abort;
    function next() {
      const value = buffer.shift();
      consumer.processing = false;
      if (!value) return;
      consumer.processing = true;
      fn(value, next);
    }
    function abort() {
      const index = consumers.indexOf(consumer);
      if (index === -1) return;

      consumers.splice(index, 1);
    }
  }
}

export namespace Stream {
  export type Consumer<VALUE> = {
    next: () => void;
    buffer: VALUE[];
    processing: boolean;
    fn: (value: VALUE, next: () => void) => void;
  };
}

const MAX = 1_000_000;

const stream = new Stream<number>();
const oldStream = new OldStream<number>();

oldStream.listen((value) => {
  // await new Promise((r) => setTimeout(r, Math.random() * 200));
  if (value === MAX) console.log("old stream", Math.round(performance.now() - now));
});
stream.listen((value, next) => {
  // await new Promise((r) => setTimeout(r, Math.random() * 200));
  if (value === MAX) {
    console.log("new stream", Math.round(performance.now() - now));
    return;
  }
  next();
});

const now = performance.now();
(async () => {
  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
    // oldStream.push(i);
  }
})();
