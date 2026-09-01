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
import { scope } from "./transformers/scope.ts";
import { tap } from "./transformers/tap.ts";
import { merge } from "./transformers/merge.ts";
import { replayLatest } from "./transformers/replay-latest.ts";
import { share } from "./transformers/share.ts";
import { replay } from "./transformers/replay.ts";
import { DefaultQueue } from "./core/default-queue.ts";
import { take } from "./transformers/take.ts";
import { toArray } from "./transformers/to-array.ts";
import { range } from "./transformers/range.ts";
import { index } from "./transformers/index.ts";
import { print } from "./transformers/print.ts";
import { pace } from "./transformers/pace.ts";
import { buffer } from "./transformers/buffer.ts";
import { latests } from "./transformers/latests.ts";
import { delay } from "./transformers/delay.ts";
import { context } from "./transformers/context.ts";
import { gate } from "./transformers/gate.ts";
import { of } from "./sources/of-source.ts";
import { listen } from "./transformers/listen.ts";
import { first } from "./transformers/first.ts";
import { last } from "./transformers/last.ts";
import { tapBatch } from "./transformers/tap-batch.ts";
import { reduce } from "./transformers/reduce.ts";
import { safe } from "./transformers/safe.ts";
import { distinct } from "./transformers/distinct.ts";
import { find } from "./transformers/find.ts";
import { toConsole } from "./transformers/to-console.ts";
import { fromRange } from "./sources/range-source.ts";
import { fromFunction } from "./sources/function-source.ts";
import { switch$ } from "./transformers/switch$.ts";
import { combine } from "./transformers/combine.ts";
import { pump } from "./transformers/pump.ts";
import { scan } from "./transformers/scan.ts";
import { debounce } from "./transformers/debounce.ts";
import { every } from "./transformers/every.ts";
import { max } from "./transformers/max.ts";
import { min } from "./transformers/min.ts";
import { count } from "./transformers/count.ts";
import { terminate } from "./transformers/terminate.ts";
import { pipe } from "./transformers/pipe.ts";
import { sum } from "./transformers/sum.ts";
import { Error } from "./core/types.ts";

function asyncValue<T>(value: T, ms?: number) {
  return new Promise<T>((res, rej) =>
    setTimeout(() => (value instanceof Error ? rej(value) : res(value)), ms ?? Math.random() * 500),
  );
}
function consumerBench() {
  const MAX = 300_000_000;

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
    consumer.terminate("abort");
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

// rxjsBench(); // rxjs: 1 000 000 push -> 100 stages in 2469 ms
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

// streamBench(); //stream: 1 000 000 push -> 100 stages in 838 ms

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
  const $source = fromTimeout(1000, "hey");
  const stream = Stream.from($source);
  stream
    .consume((self, value) => {
      console.log("c1", value);
      self.next();
    })
    .next();
}

// fromTimeoutTest();

function fromRangeTest() {
  fromRange(20, 25).pipe(toConsole());
}
// fromRangeTest();

function fromFunctionTest() {
  fromFunction(() => Math.floor(Math.random() * 10))
    .pipe(toConsole())
    .pipe(toConsole())
    .pipe(toConsole());
}
// fromFunctionTest();

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
  fromIterable([asyncValue(1), Promise.reject(2), asyncValue(3)])
    .pipe(resolve(2))

    .consume((c, v) => {
      console.log(v);
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

function scopeTest() {
  const s1 = fromTimeout(1600);
  const s2 = fromTimeout(600);

  fromInterval(500)
    .pipe(map(() => Math.floor(Math.random() * 10 + 1)))
    .pipe(scope(s1, s2))
    .pipe(tap(console.log))
    .pipe(listen());
}

// scopeTest();

function mergeTest() {
  const s1 = fromInterval(500).pipe(map((_, index) => `${index}S1`));
  const s3 = fromInterval(1000).pipe(map((_, index) => `${index * 100}S3`));
  const s2 = fromInterval(1200)
    .pipe(map((_, index) => index.toFixed(3) + "S2"))
    .pipe(merge(s1, s3))
    .pipe(tap(console.log))
    .pipe(listen());
}

// mergeTest();

function replayLatestTest() {
  const s1 = fromInterval(200)
    .pipe(map((_, index) => index))
    .pipe(share())
    .pipe(replayLatest(3));

  setTimeout(() => {
    s1.pipe(listen(console.log));
  }, 2000);
}
// replayLatestTest();

function toArrayTest() {
  fromInterval(300)
    .pipe(map((_, i) => i))
    .pipe(take(10))
    .pipe(toArray())
    .pipe(listen(console.log));
}

// toArrayTest();

async function rangeTest() {
  fromIterable(["a", "b", "c", "d", "e", "f", "g", "h"]).pipe(range(1, 2)).pipe(toArray()).pipe(listen(console.log));
}

// rangeTest();

function paceTest() {
  fromInterval(100).pipe(index()).pipe(range(3, 3)).pipe(pace(1000)).pipe(print()).pipe(listen());
}
// paceTest();

function bufferTest() {
  fromIterable([1, 2, 3, 4, 5, 6, 7, 8, 9]).pipe(buffer(2)).pipe(print()).pipe(listen());
}
// bufferTest();

function latestsTest() {
  const s = fromInterval(300).pipe(index()).pipe(share());

  const buffered = s.pipe(latests(2));

  setTimeout(() => {
    buffered.pipe(print()).pipe(listen());
  }, 2000);
}

// latestsTest();

function contextTest() {
  fromIterable([1, 2, 3, 4, 5])
    .pipe(context({ count: 100 }))
    .pipe(tap((v) => v.context.count++))
    .pipe(print())
    .pipe(listen());
}

// contextTest();

function gateTest() {
  const control = fromInterval(3000).pipe(map((_, i) => i % 2 === 0));
  fromInterval(500).pipe(gate(control)).pipe(index()).pipe(print()).pipe(listen());
}

// gateTest();

function listenTest() {
  of(1, 2, 3, 4, 5).pipe(listen(console.log));
}

// listenTest();

function firstTest() {
  of(1, 2, 3, 4, 5).pipe(first()).pipe(listen(console.log));
}
// firstTest();
function lastTest() {
  of(1, 2, 3, 4, 5).pipe(last()).pipe(listen(console.log));
}
// lastTest();

function reduceTest() {
  of(1, 2, 3, 4)
    .pipe(reduce(0, (acc, v) => acc + v))
    .pipe(listen(console.log));
}
// reduceTest();

function safeTest() {
  of(1, 2, 3, 4)
    .pipe(
      safe(
        map((v) => {
          if (v === 3) throw "kechmahaja";
          return v.toFixed(2);
        }),
      ),
    )
    .pipe(filter((v) => !(v instanceof Error)))
    .pipe(toConsole());
}

// safeTest();

function distinctTest() {
  of({ id: 1, name: "a" }, { id: 1, name: "b" }, { id: 2, name: "a" })
    .pipe(distinct((v) => v.name))
    .pipe(listen(console.log));
}

// distinctTest();

function findTest() {
  of(1, 2, 3, 4, 5)
    .pipe(find((v) => v > 3))
    .pipe(toConsole());
}

// findTest();

function switchTest() {
  const s1 = fromInterval(300).pipe(map(() => "a"));
  const s2 = fromInterval(300).pipe(map(() => "b"));
  const s3 = fromInterval(300).pipe(map(() => "c"));
  // .pipe(scope(fromTimeout(3000)));

  of(1, 2, s3, fromTimeout(400, 44)).pipe(delay(2000)).pipe(switch$()).pipe(toConsole());
}

// switchTest();

function combineTest() {
  const s1 = of(1, 2, 3, 4).pipe(delay(100));
  const s2 = of("a", "b", "c", "d").pipe(delay(200));

  s1.pipe(combine(s2)).pipe(toConsole());
}
// combineTest();

function pumpTest() {
  const s = of(1, 2, 3).pipe(tap(console.log)).pipe(pump());

  // s.pipe(toConsole());
}
// pumpTest();

function scanTest() {
  of(1, 2, 3, 4)
    .pipe(scan(0, (acc, v) => acc + v))
    .pipe(toConsole());
}

// scanTest();

function debounceTest() {
  const s = new Stream();
  s.pipe(debounce(1000)).pipe(toConsole());

  of(1, 2, 3, 4)
    .pipe(delay(200))
    .pipe(listen((v) => s.push(v)));
}

// debounceTest();

function everyTest() {
  of(1, 2, 3, 4)
    .pipe(every((v) => v <= 4))
    .pipe(toConsole());
}

// everyTest();
function maxTest() {
  of(1, 2, 3, 4, 2, 3, 9, 3, 2, 1).pipe(max()).pipe(toConsole());
}

// maxTest();
function minTest() {
  of(1, 2, 3, 4, 2, 3, 9, 3, -3, 2, 1).pipe(min()).pipe(toConsole());
}

// minTest();
function sumTest() {
  of(1, 2, 3, 4, 2, 3, 9, 3, -3, 2, 1)
    .pipe(sum())
    .pipe(pace(100))
    .pipe(tap((v, i) => console.log(`step ${i}:${v}`)))
    .pipe(last())
    .pipe(toConsole());
}
sumTest();

function countTest() {
  of(1, 2, 3, 4).pipe(count()).pipe(toConsole());
}

// countTest();
function terminateTest() {
  of(1, 2, 3, 4).pipe(terminate()).pipe(toConsole());
}

// terminateTest();

function pipeTest() {
  of(1, 2, 3, 4)
    .pipe(map((v) => v * 100))
    .pipe(share())
    .pipe(pipe((input) => input.pipe(passive()).pipe(toConsole())))
    .pipe(listen());
}

// pipeTest();
