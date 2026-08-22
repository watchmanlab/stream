import { Subject, tap as rxtap, map as rxmap, filter as rxfilter, Observable, single } from "rxjs";
import { Consumer } from "./core/consumer.ts";

function asyncValue<T>(value: T, ms?: number) {
  return new Promise<T>((res, rej) =>
    setTimeout(() => (value instanceof Error ? rej(value) : res(value)), ms ?? Math.random() * 500),
  );
}
function consumerBench() {
  const MAX = 200_000_000;

  const start = performance.now();

  const consumer = new (class extends Consumer<number> {
    override handler(consumer: Consumer<number>, value: number): void {
      if (value === MAX) console.log("class", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      Consumer.next(consumer);
    }
  })();

  Consumer.next(consumer);

  const push = (i: number) => Consumer.push(consumer, i);

  for (let i = 0; i <= MAX; i++) {
    push(i);
  }
}

consumerBench(); //class 200 000 000 1107 ms

function consumerBench2() {
  const MAX = 10_000_000;

  const start = performance.now();

  for (let i = 0; i <= MAX; i++) {
    const consumer = new Consumer<number>((self, v) => {
      if (v === MAX) {
        self.terminate("complete");
        return;
      }

      self.next();
    });
    consumer.terminate("complete");
  }
  console.log(Math.round(performance.now() - start), "ms");
}

// consumerBench2(); // class 578 ms, closur 2231 ms
function consumerTest() {
  const consumer = new Consumer<number>((consumer, value) => {
    if (value === 2) {
      setTimeout(() => {
        console.log(value);
        consumer.next();
      }, 1000);
      return;
    }

    console.log(value);
    consumer.next();
  });

  consumer.next();
  consumer.push(1);
  consumer.push(2);
  consumer.push(3);
}
// consumerTest();
function rxjsBench() {
  const MAX = 1_000_000;
  const STAGES = 100;

  const subject = new Subject<number>();

  let chain: Observable<number> = subject;

  for (let i = 0; i < STAGES; i++) {
    chain = chain.pipe(rxmap((v) => (v * v) / v));
  }

  const start = performance.now();

  chain.subscribe((v) => {
    if (v === MAX)
      console.log(
        "rxjs:",
        `${v.toLocaleString("fr")} push ->`,
        `${STAGES} stages in`,
        Math.round(performance.now() - start),
        "ms",
      );
  });

  for (let i = 0; i <= MAX; i++) {
    subject.next(i);
  }
}

// rxjsBench(); // rxjs: 1 000 000 push -> 100 stages in 2474 ms
function streamBench() {
  const MAX = 1_000_000;
  const STAGES = 100;

  const stream = new Stream<number>();

  let chain: Source<number> = stream;

  for (let i = 0; i < STAGES; i++) {
    chain = chain.pipe(map((v) => (v * v) / v));
  }

  const start = performance.now();

  chain
    .consume((consumer, v) => {
      if (v === MAX)
        console.log(
          "stream:",
          `${v.toLocaleString("fr")} push ->`,
          `${STAGES} stages in`,
          Math.round(performance.now() - start),
          "ms",
        );
      consumer.next();
    })
    .next();

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
}

// streamBench(); //stream: 1 000 000 push -> 100 stages in 21 ms

function streamTest() {
  const stream = fromIterable([1, 2, 3]);
  const stream2 = Stream.from(stream);
  stream2
    .consume({
      handler: (consumer, value) => {
        console.log("c1", value);
        consumer.next();
      },
    })
    .next();

  // stream.push(1);
  // stream.push(2);
  // stream.push(3);
}

// streamTest();
// c1 1
// c1 2
// c1 3
