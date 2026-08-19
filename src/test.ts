import { Subject, tap as rxtap, map as rxmap, filter as rxfilter, Observable, single } from "rxjs";
import { Consumer } from "./core/consumer.ts";
import { Stream } from "./core/stream.ts";
import { fromIterator } from "./sources/iterator-source.ts";
import { fromIterable } from "./sources/iterable-source.ts";
import { fromGenerator } from "./sources/generator-source.ts";
import { fromAsyncIterator } from "./sources/async-iterator-source.ts";
import { fromAsyncIterable } from "./sources/async-iterable-source.ts";
import { fromAsyncGenerator } from "./sources/async-generator-source.ts";
import { fromAbortSignal } from "./sources/abort-signal-source.ts";
import { fromAbortController } from "./sources/abort-controller-source.ts";
import { fromEventTarget } from "./sources/event-target-source.ts";
import { fromPromise } from "./sources/promise-source.ts";

import { map } from "./transformers/map";
import { filter } from "./transformers/filter";
import { tap } from "./transformers/tap";
import { pump } from "./transformers/pump";
import { fromInterval } from "./sources/interval-source.ts";
import { fromTimeout } from "./sources/timeout-source.ts";
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
import { merge } from "./transformers/merge.ts";
import { tick } from "./transformers/tick.ts";
import { flat$ } from "./transformers/flat$.ts";
import { pace } from "./transformers/pace.ts";
import { debounce } from "./transformers/debounce.ts";
import { keepNewest } from "./transformers/keep-newest.ts";

function asyncValue<T>(value: T, ms?: number) {
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

// rxjsBench(); // rxjs: 1 000 000 push -> 100 stages in 2518 ms
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

// streamBench(); //stream: 1 000 000 push -> 100 stages in 1231 ms

function streamTest() {
  const stream = fromIterable([1, 2, 3]);
  const stream2 = Stream.from(stream);
  // stream2
  //   .consume((consumer, value) => {
  //     console.log("c1", value);
  //     consumer.next();
  //   })
  //   .next();
  (async () => {
    for await (const value of stream) {
      console.log(value);
    }
  })();

  // stream.push(1);
  // stream.push(2);
  // stream.push(3);
}

// streamTest();
// c1 1
// c1 2
// c1 3

function fromIteratorTest() {
  const $source = fromIterator([1, 2, 3].values());
  const stream = Stream.from($source);
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
  const stream = Stream.from($source);
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
  const stream = Stream.from($source);
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
  const stream = Stream.from($source);
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
  const stream = Stream.from($source);
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
  const stream = Stream.from($source);
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
  const stream = Stream.from($source);
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
  const stream = Stream.from($source);
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
  const stream = Stream.from($source);
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
  const stream = Stream.from($source);
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
  const stream = Stream.from($source);
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
  const stream = Stream.from($source);
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
  fromIterable([asyncValue(1), asyncValue(2), asyncValue(3)])
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

  // $stream
  //   .consume((c, v) => {
  //     console.log(v);
  //     c.next();
  //   })
  //   .next();

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

// passiveTest();

function mergeTest() {
  const s1 = fromIterable([1, 2, 3]);
  const s2 = fromIterable(["a", "b", "c"]);
  const s3 = fromIterable(["foo", "bar", "baz"]);

  s1.pipe(merge(s2))
    .pipe(merge(s3))
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();
}

// mergeTest();

function streamFromTest() {
  const stream = Stream.from(fromIterable([1, 2, 3]));

  stream.$push
    .consume((c, v) => {
      console.log("c1", v);
      c.next();
    })
    .next();
  stream
    .consume((c, v) => {
      console.log("c2", v);
      c.next();
    })
    .next();
}

// streamFromTest();

function flat$Test() {
  fromIterable([
    fromIterable([1, 2, 3]),
    fromIterable([4, 5, 6, fromIterable(["a", "b", "c"] as const)]),
    fromIterable([7, 8, 9]),
  ])
    .pipe(flat$(2))
    .consume((c, v) => {
      //(parameter) v: number | "a" | "b" | "c"
      console.log(v);
      c.next();
    })
    .next();
}

// flat$Test();

function paceTest() {
  const $stream = new Stream<number>();

  $stream
    .pipe(pace(1000))
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();

  {
    (async () => {
      $stream.push(await asyncValue(1, 100));
      $stream.push(await asyncValue(2, 500));
      $stream.push(await asyncValue(3, 1000));
    })();
  }
}
// paceTest();

function keepNewestTest() {
  const stream = new Stream<number>();
  stream
    .pipe(keepNewest(1))

    .consume(async (c, v) => {
      await asyncValue(3, 100);
      console.log(v);
      c.next();
    })
    .next();

  stream.push(1);
  stream.push(2);
  stream.push(3);
}
keepNewestTest();
function debounceTest() {
  const $stream = new Stream<number>();

  $stream
    .pipe(debounce(1000))
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();

  {
    (async () => {
      $stream.push(await asyncValue(1, 100));
      $stream.push(await asyncValue(2, 500));
      $stream.push(await asyncValue(3, 1000));
    })();
  }
}
// debounceTest();
