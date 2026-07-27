import { Stream } from "./core/stream";
import { Consumer } from "./core/consumer";
import { map, Map } from "./transformers/map";
import { fromIterator, FromIterator } from "./streams/from-iterator";
import { fromIterable } from "./streams/from-iterable";
import { fromEventTarget } from "./streams/from-event-target";
import { filter } from "./transformers/filter";
import { resolve } from "./transformers/resolve";
import { tap } from "./transformers/tap";
import { pump } from "./transformers/pump";
import { auditTime } from "./transformers/audit-time";
import { fromAsyncGenerator } from "./streams/from-async-generator";
import { passive } from "./transformers/passive";

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

function streamBench() {
  const MAX = 100_000_000;

  const start = performance.now();
  const stream = new Stream<number>();
  stream
    .consume((consumer, v) => {
      if (v === MAX) {
        console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
        consumer.terminate("complete");
        return;
      }

      consumer.next();
    })
    .next();

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
}

// streamBench(); //100 000 000 943 ms

function streamTest() {
  const stream = new Stream<number>();
  const stream2 = new Stream({ source: stream });
  stream2
    .consume((consumer, value) => {
      if (value === 2) {
        setTimeout(() => {
          console.log("c1", value);
          consumer.next();
        }, 500);
        // consumer.next();
        return;
      }
      console.log("c1", value);
      consumer.next();
    })
    .next();
  // stream
  //   .consume((consumer, value) => {
  //     console.log("L2", value.toString().repeat(3));

  //     consumer.next();
  //   })
  //   .next();

  stream.push(1);
  stream.push(2);
  stream.push(3);
}

// streamTest();
// c1 1
// c1 2
// c1 3

function fromIteratorTest() {
  fromIterator([1, 2, 3].values())
    .consume((c, v) => {
      console.log("c1", v);
      c.next();
    })
    .next();
}

// fromIteratorTest();
function fromIterableTest() {
  const stream = fromIterable([1, 2, 3]);
  stream
    .consume((c, v) => {
      console.log("c1", v);
      c.next();
    })
    .next();
}

// fromIterableTest();

function fromEventTargetTest() {
  const et = new EventTarget();

  fromEventTarget(et, "click")
    .consume((consumer, value) => {
      setTimeout(() => {
        console.log(value.type);

        consumer.next();
      }, 1000);
    })
    .next();
  et.dispatchEvent(new Event("click"));
  et.dispatchEvent(new Event("click"));
  et.dispatchEvent(new Event("click"));
}

// fromEventTargetTest();
// click
// click
// click

function mapTest() {
  const stream = new Stream<number>();

  fromIterable([1, 2, 3])
    .pipe(map((v) => v.toString(), { name: "$map2", source: fromIterable(["a", "b", "c"]) }))
    .consume((c, v) => {
      console.log(v);
      setTimeout(() => {
        c.next();
      }, 1000);
    })
    .next();

  // stream
  //   .pipe(map((v) => new Promise<number>((resolve) => setTimeout(() => resolve(v * 100), Math.random() * 500))))
  //   .consume(async (c, v) => {
  //     console.log(await v);
  //     c.next();
  //   })
  //   .next();

  // stream.push(1);
  // stream.push(2);
  // stream.push(3);
}
// mapTest();

function filterTest() {
  const stream = new Stream<number>();
  stream
    .pipe(filter((v) => v % 2 == 0))
    .pipe(tap((value) => console.log(value)))
    .pipe(pump())
    .traversal.$tap.$filter.$rejected.pipe(tap((v) => console.log("rejected", v)))
    .pipe(pump());

  stream.push(1);
  stream.push(2);
  stream.push(3);
  stream.push(4);
  stream.push(5);
  stream.push(6);
}

// filterTest();

function resolveTest() {
  const stream = fromIterable([1, 2, 3, 4])
    .pipe(map((v) => new Promise<number>((r) => setTimeout(() => r(v), Math.random() * 1000))))
    .pipe(resolve())
    .pipe(tap((value) => console.log(value)))
    .pipe(pump());

  stream.traversal.$tap.$resolve.$map.$iterable;
}

// resolveTest();

function auditTimeTest() {
  fromAsyncGenerator(async function* () {
    yield 1;
    yield 2;
    yield 3;
  })
    .pipe(passive())
    .pipe(auditTime(500))
    .pipe(tap((v) => console.log("audit", v)))
    .pipe(pump())
    .traversal.$tap.$auditTime.$passive.$asyncGenerator.pipe(tap((v) => console.log("driver", v)))
    .pipe(pump());
}

auditTimeTest();
// driver 1
// driver 2
// driver 3
// audit 1

function passiveTest() {
  const stream = fromAsyncGenerator(async function* () {
    yield 1;
    yield 2;
    yield 3;
  });
  const activePipeline = stream
    .pipe(map((v) => v * 100))
    .pipe(tap((v) => console.log("active pipeline", v)))
    .pipe(pump());

  const passivePipeline = activePipeline.traversal.$tap.$map.$asyncGenerator
    .pipe(passive())
    .pipe(tap((v) => console.log("passive pipeline", v)))
    .pipe(pump());
}

// passiveTest();
// passive pipeline 1
// active pipeline 100
// passive pipeline 2
// active pipeline 200
// passive pipeline 3
// active pipeline 300
