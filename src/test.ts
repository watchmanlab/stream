import { Subject, tap as rxtap, map as rxmap, filter as rxfilter, Observable, single } from "rxjs";
import { Consumer } from "./core/consumer.ts";
import { Stream } from "./core/stream.ts";
import { Source } from "./core/source";
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
import { fromInterval } from "./sources/interval-source.ts";
import { fromTimeout } from "./sources/timeout-source.ts";

import { map } from "./transformers/map";
import { takeUntil } from "./transformers/take-until.ts";
import { filter } from "./transformers/filter.ts";
import { skip } from "./transformers/skip.ts";
import { skipWhile } from "./transformers/skip-while.ts";
import { skipUntil } from "./transformers/skip-until.ts";
import { resolve } from "./transformers/resolve.ts";
import { passive } from "./transformers/passive.ts";
import { zip } from "./transformers/zip.ts";
import { dependOn } from "./transformers/depend-on.ts";
import { tap } from "./transformers/tap.ts";
import { pump } from "./transformers/pump.ts";
import { merge } from "./transformers/merge.ts";

function asyncValue<T>(value: T, ms?: number) {
  return new Promise<T>((res, rej) =>
    setTimeout(() => (value instanceof Error ? rej(value) : res(value)), ms ?? Math.random() * 500),
  );
}
function consumerBench() {
  const MAX = 200_000_000;

  const start = performance.now();

  const consumer = new Consumer<number>((self, v) => {
    if (v === MAX) console.log("class", v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
    self.next();
  });

  consumer.next();

  for (let i = 0; i <= MAX; i++) {
    consumer.push(i);
  }
}

// consumerBench(); //class 200 000 000 827 ms

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

  const mapped = stream.pipe(map((v) => v * 3)).pipe(map((v) => v.toFixed(2)));
  mapped
    .consume((consumer, value) => {
      console.log("c1", value);
      consumer.next();
    })
    .next();

  stream.push(1).push(2).push(3);
}
// mapTest();

function filterTest() {
  const filtered = fromIterable([1, 2, 3, 4, 5, 6, 7, 8, 9]).pipe(filter((v) => v % 2 === 0));

  filtered.$complements
    .consume((c, v) => {
      console.log("complements", v);
      c.next();
    })
    .next();

  filtered
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();
}
// filterTest();

function tapBatchTest() {
  fromIterable([[1, 2], [3]])
    .pipe(tapBatch((v) => console.log(v)))
    .consume((c, v) => {
      // console.log(v);
      c.next();
    })
    .next();
}

async function takeUntilTest() {
  const stream = new Stream();
  const notifier = new Stream();
  asyncValue(2, 1000).then((v) => notifier.push(v));
  stream
    .pipe(takeUntil(notifier))
    .consume((c, v) => {
      console.log(v);

      c.next();
    })
    .next();

  stream.push(await asyncValue(1, 500));
  stream.push(await asyncValue(2, 500));
}
// takeUntilTest();

function skipTest() {
  fromIterable([1, 2, 3, 4])
    .pipe(skip(1))
    .pipe(skipWhile((v) => v <= 3))
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();
}

// skipTest();

async function skipUntilTest() {
  const stream = new Stream();
  const notifier = new Stream();

  asyncValue("", 600).then(() => notifier.push(""));

  stream
    .pipe(skipUntil(notifier))
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();

  stream.push(await asyncValue(1, 500));
  stream.push(await asyncValue(2, 500));
}
// skipUntilTest();

function resolveTest() {
  fromIterable([asyncValue(1), asyncValue(2), asyncValue(3)])
    .pipe(resolve(1))
    .consume((c, v) => {
      console.log(v.value);
      c.next();
    })
    .next();
}

// resolveTest();

function passiveTest() {
  const source = fromIterable([1, 2, 3]);

  source.consume((c, v) => {
    console.log(v);
    c.next();
  });
  // .next();
  source
    .pipe(passive())
    .consume((c, v) => {
      console.log("passive", v);
      c.next();
    })
    .next();

  // source.push(1);
  // source.push(2);
  // source.push(3);
}

// passiveTest();

function zipTest() {
  const s1 = new Stream<number>();
  const s2 = new Stream<string>();
  const s3 = new Stream<boolean>();

  const zipped = s1.pipe(zip(s2, s3));
  zipped
    .consume((c, v) => {
      console.log(v);
      c.next();
    })
    .next();

  zipped.$rest
    .consume((c, v) => {
      console.log("rest", v);
      c.next();
    })
    .next();

  asyncValue(1, 500).then((v) => s1.push(v));
  asyncValue(2, 700).then((v) => s1.push(v));
  asyncValue(3, 900).then((v) => s1.push(v));

  asyncValue("a", 500).then((v) => s2.push(v));
  // asyncValue("b", 700).then((v) => s2.push(v));
  asyncValue("c", 900).then((v) => s2.push(v));

  asyncValue(true, 500).then((v) => s3.push(v));
  asyncValue(false, 700).then((v) => s3.push(v));
  asyncValue(true, 900).then((v) => s3.push(v));

  setTimeout(() => {
    s1.terminate("abort");
  }, 1000);
}

// zipTest();
// [ 1, "a", true ]
// [ 2, "c", false ]
// rest [ 3, Symbol(empty), true ]

function dependOnTest() {
  const s1 = fromTimeout(1600);
  const s2 = fromTimeout(600);

  fromInterval(500)
    .pipe(map(() => Math.floor(Math.random() * 10 + 1)))
    .pipe(dependOn(s1, s2))
    .pipe(tap(console.log))
    .pipe(pump());
}

// dependOnTest();

function mergeTest() {
  const s1 = fromInterval(1000).pipe(map((_, index) => index));
  const s3 = fromInterval(1000).pipe(map((_, index) => index * 100));
  const s2 = fromInterval(1000)
    .pipe(map((_, index) => index.toFixed(3)))
    .pipe(merge(s1, s3))
    .pipe(tap(console.log))
    .pipe(pump());
}

// mergeTest();
