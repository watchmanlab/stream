import { Stream } from "./core/stream";

import { Consumer } from "./core/consumer";
import { fromIterator } from "./sources/from-iterator";
import { fromIterable } from "./sources/from-iterable";

function consumerBench() {
  const MAX = 500_000_000;

  const start = performance.now();

  const consumer = Consumer.create<number>((self, v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      self.terminate("complete");
      return;
    }

    self.next();
  });

  for (let i = 0; i <= MAX; i++) {
    consumer.push(i);
  }
}

// consumerBench(); //350 000 000 1000 ms

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

consumerBench2(); // class 578 ms, closur 2231 ms
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

  consumer.push(1);
  consumer.push(2);
  consumer.push(3);
}
// consumerTest();

function streamBench() {
  const MAX = 1_000_000;

  const start = performance.now();
  const stream = new Stream<number>();
  stream.listen((consumer, v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      consumer.terminate("complete");
      return;
    }

    consumer.next();
  });

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
}

// streamBench(); //1 000 000 13 ms

function streamTest() {
  const stream = new Stream<number>();

  // stream.listen((consumer, value) => {
  //   if (value === 2) {
  //     setTimeout(() => {
  //       console.log("c1", value);
  //       consumer.next();
  //     }, 1000);
  //     // consumer.next();
  //     return;
  //   }
  //   console.log("c1", value);
  //   consumer.next();
  // });
  stream.listen((consumer, value) => {
    console.log("L2", value.toString().repeat(3));
    if (value < 4) stream.push(++value);
    consumer.next();
  });

  stream.push(1);
  // stream.push(2);
  // stream.push(3);
}

// streamTest();

function fromIteratorTest() {
  const stream = fromIterator([1, 2, 3].values());

  stream.listen((c, v) => {
    console.log("c1", v);
    c.next();
  });

  stream.listen((c, v) => {
    console.log("c2", v.toString().repeat(3));
    c.next();
  });
}

// fromIteratorTest();
function fromIterableTest() {
  const stream = fromIterable([1, 2, 3]);

  stream.listen((c, v) => {
    console.log("c1", v);
    c.next();
  });

  stream.listen((c, v) => {
    console.log("c2", v.toString().repeat(3));
    c.next();
  });
}

// fromIterableTest();
