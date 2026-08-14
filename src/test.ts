import { Subject, tap as rxtap, map as rxmap, filter as rxfilter, Observable } from "rxjs";
import { Consumer } from "./core/consumer.ts";
import { Producer } from "./core/producer.ts";
import { fromIterator } from "./sources/from-iterator";
import { fromIterable } from "./sources/from-iterable";
import { fromGenerator } from "./sources/from-generator";
import { fromAsyncIterator } from "./sources/from-async-iterator";
import { fromAsyncIterable } from "./sources/from-async-iterable";
import { fromAsyncGenerator } from "./sources/from-async-generator";
import { fromAbortSignal } from "./sources/from-abort-signal";
import { fromAbortController } from "./sources/from-abort-controller";
import { fromEventTarget } from "./sources/from-event-target";
import { fromPromise } from "./sources/from-promise";

import { map } from "./transformers/map";
import { filter } from "./transformers/filter";
import { tap } from "./transformers/tap";
import { pump } from "./transformers/pump";
import { fromInterval } from "./sources/from-interval";
import { fromTimeout } from "./sources/from-timeout";
import { Consumable } from "./core/types";
import { Source } from "./core/source";

function consumerBench() {
  const MAX = 350_000_000;

  const start = performance.now();

  const consumer = new Consumer<number>((self, v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      self.terminate("complete");
      return;
    }

    self.next();
  });

  consumer.next();
  for (let i = 0; i <= MAX; i++) {
    consumer.push(i);
  }
}

// consumerBench(); //350 000 000 989 ms

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
  const STAGES = 20;

  const subject = new Subject<number>();
  let chain: Observable<number> = subject.pipe(rxmap((v) => v));

  for (let i = 1; i < STAGES; i++) {
    chain = chain.pipe(rxmap((v) => v));
  }

  const start = performance.now();

  chain.subscribe((v) => {
    if (v === MAX) console.log("rxjs:  ", v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
  });

  for (let i = 0; i <= MAX; i++) {
    subject.next(i);
  }
}

// rxjsBench(); //rxjs:   1 000 000 362 ms
function streamBench() {
  const MAX = 1_000_000;
  const STAGES = 100;

  const stream = new Producer<number>();

  let chain: Source<number> = stream.pipe(map((v) => v));

  for (let i = 0; i < STAGES; i++) {
    chain = chain.pipe(map((v) => v));
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

streamBench(); //stream: 1 000 000 push -> 200 stages in 1037 ms

function streamTest() {
  const stream = new Producer<number>();
  const stream2 = new Producer({ source: stream });
  stream2
    .consume((consumer, value) => {
      // if (value === 2) {
      //   setTimeout(() => {
      //     console.log("c1", value);
      //     consumer.next();
      //   }, 500);
      //   // consumer.next();
      //   return;
      // }
      console.log("c1", value);
      consumer.next();
    })
    .next();
  (async () => {
    for await (const value of stream2) {
      console.log("c2", value);
    }
  })();

  stream.push(1);
  stream.push(2);
  stream.push(3);
}

// streamTest();
// c1 1
// c1 2
// c1 3

function fromIteratorTest() {
  const source = fromIterator([1, 2, 3].values());
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log(value);
      self.next();
    })
    .next();
}

// fromIteratorTest();
function fromIterableTest() {
  const source = fromIterable([1, 2, 3]);
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log(value);
      self.next();
    })
    .next();
}

// fromIterableTest();
function fromGeneratorTest() {
  const source = fromGenerator(function* () {
    yield 1;
    yield 2;
    yield 3;
  });
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log(value);
      self.next();
    })
    .next();
}

// fromGeneratorTest();

function fromAsyncIteratorTest() {
  let counter = 0;
  const source = fromAsyncIterator({
    next() {
      return Promise.resolve(++counter === 4 ? { value: undefined, done: true } : { value: counter });
    },
  });
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log(value);
      self.next();
    })
    .next();
}

// fromAsyncIteratorTest();
function fromAsyncIterableTest() {
  const source = fromAsyncIterable({
    [Symbol.asyncIterator]() {
      let counter = 0;
      return {
        next() {
          return Promise.resolve(++counter === 4 ? { value: undefined, done: true } : { value: counter });
        },
      };
    },
  });
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log(value);
      self.next();
    })
    .next();
}

// fromAsyncIterableTest();
function fromAsyncGeneratorTest() {
  const source = fromAsyncGenerator(async function* () {
    await new Promise((r) => setTimeout(r, Math.random() * 500));
    yield 1;
    await new Promise((r) => setTimeout(r, Math.random() * 500));
    yield 2;
    await new Promise((r) => setTimeout(r, Math.random() * 500));
    yield 3;
  });
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log(value);
      self.next();
    })
    .next();
}

// fromAsyncGeneratorTest();

function fromAbortSignalTest() {
  const controller = new AbortController();

  const source = fromAbortSignal(controller.signal);
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log("aborted", value);
      self.next();
    })
    .next();

  controller.abort();
  console.log(stream.status);
}

// fromAbortSignalTest();
function fromAbortControllerTest() {
  const controller = new AbortController();

  controller.abort();

  const source = fromAbortController(controller);
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log("aborted", value);
      self.next();
    })
    .next();

  console.log(stream.status);
}

// fromAbortControllerTest();
function fromEventTargetTest() {
  const et = new EventTarget();

  const source = fromEventTarget(et, "click");
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log(value.type);
      self.next();
    })
    .next();

  et.dispatchEvent(new Event("click"));
  et.dispatchEvent(new Event("click"));
}

// fromEventTargetTest();
function fromPromiseTest() {
  const source = fromPromise(new Promise((r) => setTimeout(() => r(33), 200)));
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log("c1", value.value);
      self.next();
    })
    .next();
  setTimeout(() => {
    stream
      .consume((self, value) => {
        console.log("c2", value.value);
        self.next();
      })
      .next();
  }, 1000);
}

// fromPromiseTest();

function fromIntervalTest() {
  const source = fromInterval(500);
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log("c1", value);
      self.next();
    })
    .next();
}

// fromIntervalTest();
function fromTimeoutTest() {
  const source = fromTimeout(1000);
  const stream = new Producer({ source });
  stream
    .consume((self, value) => {
      console.log("c1", value);
      self.next();
    })
    .next();
}

// fromTimeoutTest();

function mapTest() {
  const stream = new Producer<number>();

  const mapped = stream.pipe(map((v) => (v * 3).toFixed(3))).asProducer();
  mapped
    .consume((consumer, value) => {
      console.log("c1", value);
      consumer.next();
    })
    .next();

  const c = mapped.consume((consumer, value) => {
    setTimeout(() => {
      console.log("c2", value);
      consumer.next();
    }, 1000);
  });

  setTimeout(() => {
    c.next();
  }, 1000);

  stream.push(1).push(2).push(3);
}
// mapTest();
// c1 3.000
// c1 6.000
// c1 9.000
// ...wait 1s
// c2 3
// ...wait 1s
// c2 6
// ...wait 1s
// c2 9
