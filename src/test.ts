import { Stream } from "./core/stream";
import { Consumer } from "./core/consumer";
import { map, Map } from "./transformers/map";
import { IteratorStream } from "./streams/iterator-stream";
import { IterableStream } from "./streams/iterable-stream";
import { EventTargetStream } from "./streams/event-target-stream";

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
  new IteratorStream([1, 2, 3].values())
    .consume((c, v) => {
      console.log("c1", v);
      c.next();
    })
    .next();
}

// fromIteratorTest();
function fromIterableTest() {
  const stream = new IterableStream([1, 2, 3]);
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

  new EventTargetStream(et, "click")
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

  new IterableStream([1, 2, 3])
    .pipe(map((v) => v.toString(), { source: new IterableStream(["a", "b", "c"]) }))
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
mapTest();
