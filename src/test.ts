import { Subject, tap as rxtap, map as rxmap, filter as rxfilter, Observable, single } from "rxjs";
import { Consumer } from "./core/consumer.ts";
import { Stream } from "./core/stream.ts";
import { fromIterator } from "./sources/from-iterator";
import { fromIterable } from "./sources/from-iterable";
import { fromGenerator } from "./sources/from-generator";
import { fromAsyncIterator } from "./sources/from-async-iterator";
import { fromAsyncIterable } from "./sources/from-async-iterable";
import { fromAsyncGenerator } from "./sources/from-async-generator";
import { fromAbortSignal } from "./sources/from-abort-signal";
import { fromAbortController } from "./sources/from-abort-controller";
import { fromEventTarget } from "./sources/from-event-target.ts";
import { fromPromise } from "./sources/from-promise";

import { map } from "./transformers/map";
import { filter } from "./transformers/filter";
import { tap } from "./transformers/tap";
import { pump } from "./transformers/pump";
import { fromInterval } from "./sources/from-interval";
import { fromTimeout } from "./sources/from-timeout";
import { Consumable } from "./core/types";
import { Source } from "./core/source";
import { share } from "./transformers/share.ts";
import { Signal } from "./streams/signal.ts";
import { batch } from "./transformers/batch.ts";
import { flat } from "./transformers/flat.ts";
import { skip } from "./transformers/skip.ts";
import { resolve } from "./transformers/resolve.ts";
import { delay } from "./transformers/delay.ts";
import { tapBatch } from "./transformers/tap-batch.ts";
import { passive } from "./transformers/passive.ts";
function timeout<T>(value: T, ms?: number) {
  return new Promise<T>((res, rej) =>
    setTimeout(() => (value instanceof Error ? rej(value) : res(value)), ms ?? Math.random() * 500),
  );
}
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
  const STAGES = 100;

  const subject = new Subject<number>();

  let chain: Observable<number> = subject;

  for (let i = 1; i < STAGES; i++) {
    chain = chain.pipe(rxmap((v) => v));
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

// rxjsBench(); // rxjs: 1 000 000 push -> 100 stages in 2287 ms
function streamBench() {
  const MAX = 1_000_000;
  const STAGES = 100;

  const stream = new Stream<number>();

  let chain: Source<number> = stream;

  for (let i = 0; i < STAGES; i++) {
    chain = chain.pipe(filter((v) => v <= MAX));
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

// streamBench(); //stream: 1 000 000 push -> 100 stages in 533 ms

function streamTest() {
  const stream = new Stream<number>();
  const stream2 = new Stream({ $source: stream });
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
  const $source = fromIterator([1, 2, 3].values());
  const stream = new Stream({ $source });
  stream
    .consume((self, value) => {
      console.log(value);
      self.next();
    })
    .next();
}

// fromIteratorTest();
function fromIterableTest() {
  const $source = fromIterable([1, 2, 3]);
  const stream = new Stream({ $source });
  stream
    .consume((self, value) => {
      console.log(value);
      self.next();
    })
    .next();
}

// fromIterableTest();
function fromGeneratorTest() {
  const $source = fromGenerator(function* () {
    yield 1;
    yield 2;
    yield 3;
  });
  const stream = new Stream({ $source });
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
  const $source = fromAsyncIterator({
    next() {
      return Promise.resolve(++counter === 4 ? { value: undefined, done: true } : { value: counter });
    },
  });
  const stream = new Stream({ $source });
  stream
    .consume((self, value) => {
      console.log(value);
      self.next();
    })
    .next();
}

// fromAsyncIteratorTest();
function fromAsyncIterableTest() {
  const $source = fromAsyncIterable({
    [Symbol.asyncIterator]() {
      let counter = 0;
      return {
        next() {
          return Promise.resolve(++counter === 4 ? { value: undefined, done: true } : { value: counter });
        },
      };
    },
  });
  const stream = new Stream({ $source });
  stream
    .consume((self, value) => {
      console.log(value);
      self.next();
    })
    .next();
}

// fromAsyncIterableTest();
function fromAsyncGeneratorTest() {
  const $source = fromAsyncGenerator(async function* () {
    await new Promise((r) => setTimeout(r, Math.random() * 500));
    yield 1;
    await new Promise((r) => setTimeout(r, Math.random() * 500));
    yield 2;
    await new Promise((r) => setTimeout(r, Math.random() * 500));
    yield 3;
  });
  const stream = new Stream({ $source });
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

  const $source = fromAbortSignal(controller.signal);
  const stream = new Stream({ $source });
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

  const $source = fromAbortController(controller);
  const stream = new Stream({ $source });
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

  const $source = fromEventTarget(et, "click");
  const stream = new Stream({ $source });
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
  const $source = fromPromise(new Promise((r) => setTimeout(() => r(33), 200)));
  const stream = new Stream({ $source });
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
  const $source = fromInterval(500);
  const stream = new Stream({ $source });
  stream
    .consume((self, value) => {
      console.log("c1", value);
      self.next();
    })
    .next();
}

// fromIntervalTest();
function fromTimeoutTest() {
  const $source = fromTimeout(1000);
  const stream = new Stream({ $source });
  stream
    .consume((self, value) => {
      console.log("c1", value);
      self.next();
    })
    .next();
}

// fromTimeoutTest();

function mapTest() {
  const stream = new Stream<number>();

  const mapped = stream.pipe(map((v) => (v * 3).toFixed(3)));
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

function signalTest() {
  const $signal = new Signal();

  $signal
    .consume((consumer, value) => {
      console.log(value);
      consumer.next();
    })
    .next();

  $signal.push(1);
  $signal.push(2);
  console.log($signal.status);
}

// signalTest();

function batchTest() {
  fromIterable([1, 2, 3, 4, 5, 6, 7, 8, 9])
    .pipe(batch(2))
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();
}

// batchTest();

function flatTest() {
  fromIterable([
    [1, [2, 3]],
    [4, [5, 6]],
  ])
    .pipe(flat())
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();
}

// flatTest();

function filterTest() {
  fromIterable([1, 2, 3, 4, 5, 6, 7, 8, 9])
    .pipe(filter((v) => v % 2 === 0))
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();
}

// filterTest();

function skipTest() {
  fromIterable([1, 2, 3, 4, 5, 6])
    .pipe(skip(3))
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();
}

// skipTest();
function resolveTest() {
  fromIterable([timeout(1), timeout(2), timeout(3)])
    .pipe(resolve())
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();
}

// resolveTest();
function delayTest() {
  fromIterable([1, 2, 3])
    .pipe(delay(500))
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();
}

// delayTest();
function tapBatchTest() {
  fromIterable([[1, 2], [3]])
    .pipe(tapBatch((v) => console.log(v)))
    .consume((c, v) => {
      // console.log(v);
      c.next();
    })
    .next();
}

// tapBatchTest();

function passiveTest() {
  const $stream = new Stream<number>();

  // $stream.consume((c, v) => {
  //   console.log(v);
  //   c.next();
  // });
  // .next();

  // fromIterable([1, 2, 3])
  $stream
    .pipe(passive())
    .consume((c, v) => {
      console.log("passive", v);
      c.next();
    })
    .next();

  $stream.push(1);
  $stream.push(2);
  $stream.push(3);
}

passiveTest();
