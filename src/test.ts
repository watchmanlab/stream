import { Stream } from "./core/stream";
import { Consumer } from "./core/consumer";
import { map } from "./transformers/map";
import { fromIterator } from "./sources/from-iterator";
import { fromIterable } from "./streams/from-iterable";
import { fromEventTarget } from "./streams/from-event-target";
import { filter } from "./transformers/filter";
import { resolve } from "./transformers/resolve";
import { tap } from "./transformers/tap";
import { pump } from "./transformers/pump";
import { auditTime } from "./transformers/audit-time";
import { fromAsyncGenerator } from "./streams/from-async-generator";
import { passive } from "./transformers/passive";
import { auditCount } from "./transformers/audit-count";
import { take } from "./transformers/take";
import { skip } from "./transformers/skip";
import { replay } from "./transformers/replay";
import { tick } from "./transformers/tick";
import { takeWhile } from "./transformers/take-while";
import { takeUntil } from "./transformers/take-until";
import { takeWith } from "./transformers/take-with";
import { merge } from "./transformers/merge";
import { mapBatch } from "./transformers/map-batch";
import { flat } from "./transformers/flat";
import { Signal } from "./streams/signal";
import { debug } from "./transformers/debug";
import { batch } from "./transformers/batch";

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
  const MAX = 1_000_000;

  const start = performance.now();
  const stream = new Stream<number, "$kechma">({ name: "$kechma" });

  const chain = stream

    .pipe(
      tap((v) => {
        if (v === MAX) console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      }),
    )
    .pipe(
      tap((v) => {
        if (v === MAX) console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      }),
    )
    .pipe(
      tap((v) => {
        if (v === MAX) console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      }),
    )
    .pipe(
      tap((v) => {
        if (v === MAX) console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      }),
    )
    .pipe(map((v) => v.toFixed()))
    .pipe(map((v) => Number(v)))
    .pipe(map((v) => v.toFixed()))
    .pipe(map((v) => Number(v), { name: "$myMap" }))
    .pipe(filter((v) => v < MAX / 2, { name: "$myFilter" }))
    .pipe(pump());

  // console.log(chain.$myFilter.$myMap.$map.$map.$map.$tap.$tap.$tap.$tap.$kechma.name);

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
}

// streamBench();
// $kechma
// 1 000 000 551 ms
// 1 000 000 552 ms
// 1 000 000 552 ms
// 1 000 000 552 ms

import { Subject, tap as rxtap, map as rxmap, filter as rxfilter } from "rxjs";

function rxjsBench() {
  const MAX = 1_000_000;
  const stream$ = new Subject<number>();
  const start = performance.now();

  stream$
    .pipe(
      rxtap((v) => {
        if (v === MAX) console.log("Stage 1:", Math.round(performance.now() - start), "ms");
      }),
      rxtap((v) => {
        if (v === MAX) console.log("Stage 2:", Math.round(performance.now() - start), "ms");
      }),
      rxtap((v) => {
        if (v === MAX) console.log("Stage 3:", Math.round(performance.now() - start), "ms");
      }),
      rxtap((v) => {
        if (v === MAX) console.log("Stage 4:", Math.round(performance.now() - start), "ms");
      }),
      rxmap((v) => v.toFixed()),
      rxmap((v) => Number(v)),
      rxmap((v) => v.toFixed()),
      rxmap((v) => Number(v)),
      rxfilter((v) => v < MAX / 2),
    )
    .subscribe(); // Activates the pipeline

  for (let i = 0; i <= MAX; i++) {
    stream$.next(i);
  }
}

// rxjsBench();
// Stage 1: 103 ms
// Stage 2: 103 ms
// Stage 3: 103 ms
// Stage 4: 103 ms

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
  const source = fromIterator([1, 2, 3].values());
  const stream = new Stream({ source });
  stream
    .consume((self, value) => {
      console.log(value);
      self.next();
    })
    .next();
}

fromIteratorTest();
