import { filter } from "./transformers/filter";
import { map } from "./transformers/map";
import { Stream } from "./core/stream";
import { IterableStream } from "./streams/iterable-stream";
import { GeneratorStream } from "./streams/generator-stream";
import { resolve } from "./transformers/resolve";
import { bufferCount } from "./transformers/buffer-count";
import { Consumer } from "./core/consumer";

function consumerBench() {
  const MAX = 100_000_000;

  const start = performance.now();

  const consumer = new Consumer<number>((self, v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      self.complete();
      return;
    }

    self.next();
  });

  for (let i = 0; i <= MAX; i++) {
    consumer.push(i);
  }
}

// consumerBench(); //350 000 000 1000 ms

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
consumerTest();

function transformersBench() {
  const MAX = 7_000_000;

  const stream = new Stream<number>();

  const start = performance.now();

  const s = stream
    .pipe(map((v) => v))
    .pipe(filter((v) => v <= MAX))
    .pipe(filter((v) => v <= MAX));

  s.listen((self, v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      self.abort();
      return;
    }

    self.next();
  });
  s.listen((self, v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      self.abort();
      return;
    }

    self.next();
  });
  s.listen((self, v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      self.abort();
      return;
    }

    self.next();
  });
  s.listen((self, v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      self.abort();
      return;
    }

    self.next();
  });
  s.listen((self, v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      self.abort();
      return;
    }

    self.next();
  });

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
}

// transformersBench();
// 7 000 000 1055 ms
// 7 000 000 1055 ms
// 7 000 000 1055 ms
// 7 000 000 1055 ms
// 7 000 000 1055 ms
function mapTest() {
  const stream = new Stream<number>();

  const mapped = stream.pipe(map((value) => value.toFixed() + " mapped"));

  mapped.listen((self, v) => {
    if (v === "2 mapped") {
      setTimeout(() => {
        console.log("asynccc ", v);
        self.next();
      }, 1000);
      self.next();
      return;
    }
    console.log(v);
    self.next();
  });

  stream.push(1);
  stream.push(2);
  stream.push(3);
}
// mapTest();
function filterTest() {
  const stream = new Stream<number>();

  const mapped = stream.pipe(filter((v) => v % 2 === 0, {}));

  mapped.events.consumerJoin.listen((self) => {
    console.log("consumer join");

    self.next();
  });
  mapped.events.filtered.listen((self, value) => {
    console.log("filtered", value);
    self.next();
  });

  mapped.listen((self, v) => {
    console.log(v);

    self.next();
  });

  stream.push(1);
  stream.push(2);
  stream.push(3);
  stream.push(4);
  stream.push(5);
  stream.push(6);
}

// filterTest();
// filtered 1
// 2
// filtered 3
// 4
// filtered 5
// 6

function fromIterableTest() {
  const stream = new GeneratorStream(function* () {
    yield 1;
    yield 2;
    yield 3;
  });

  const A = stream.listen((self, v) => {
    console.log("A:", v);
    self.next();
  });
  const B = stream.listen((self, v) => {
    console.log("B:", v);
    self.next();
  });
}

// fromIterableTest();

function test() {
  const stream = new Stream<number>();

  stream.listen((self, value) => {
    console.log(value);
    self.next();
  });

  stream.push(1);
  stream.push(2);
  stream.push(3);
}
// test();

// 1
// 2
// 3

function concurrentTest() {
  const stream = new Stream<Promise<number>>();

  stream.pipe(resolve()).listen((self, v) => {
    console.log(v);
    self.next();
  });

  stream.push(new Promise<number>((r) => setTimeout(() => r(1), 200)));
  stream.push(new Promise<number>((r) => setTimeout(() => r(2), 300)));
  stream.push(new Promise<number>((r) => setTimeout(() => r(3), 100)));
}

// concurrentTest();

function bufferCountTest() {
  const stream = new Stream<number>();

  stream.pipe(bufferCount(2)).listen((self, value) => {
    console.log(value);
    self.next();
  });

  stream.push(1);
  stream.push(2);
  stream.push(3);
  stream.push(4);
  stream.push(5);
  stream.push(6);
}

// bufferCountTest();

function errorTest() {
  const stream = new Stream<number>();

  const consumer = stream.listen(
    (self, value) => {
      if (value === 2) throw new Error("kechmahaja");

      console.log(value);
      self.next();
    },
    { ready: false },
  );

  stream.push(1);
  try {
    stream.push(2);
  } catch (error) {}
  stream.push(3);

  try {
    consumer.next();
  } catch (error) {}
}

// errorTest();
