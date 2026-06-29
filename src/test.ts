import { filter } from "./transformers/filter";
import { map } from "./transformers/map";
import { Stream } from "./core/stream";
import { IterableStream } from "./streams/iterable-stream";

function bench() {
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

bench();
// 7 000 000 1149 ms
// 7 000 000 1150 ms
// 7 000 000 1150 ms
// 7 000 000 1150 ms
// 7 000 000 1150 ms
function mapTest() {
  const stream = new Stream<number>();

  const mapped = stream.pipe(map((value) => value.toFixed() + " mapped", { events: { abort(self, value) {} } }));

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

  const mapped = stream.pipe(filter((v) => v % 2 === 0));

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
  const stream = new IterableStream([1, 2, 3, 4]);

  stream.listen((self, v) => {
    console.log("A:", v);
    setTimeout(() => {
      self.next();
    }, 1000);
  });
  stream.listen((self, v) => {
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
// before
// 1
// after
// before
// 2
// after
// before
// 3
// after
