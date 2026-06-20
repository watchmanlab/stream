import { AsyncGenerator } from "../sources/async-generator";
import { AsyncIterable } from "../sources/async-iterable";
import { Iterable } from "../sources/iterable";
import { Iterator } from "../sources/iterator";
import { filter } from "../transformers/filter";
import { map } from "../transformers/map";
import { Stream } from "./stream";

function mapTest() {
  const stream = new Stream<number>();

  stream.pipe(map((v) => v * 2)).listen((self, v) => {
    console.log(v);

    self.next();
  });

  stream.push(1);
  stream.push(2);
  stream.push(3);
  stream.push(4);
}

// mapTest();
function filterTest() {
  const stream = new Stream<number>();

  const mapped = stream.pipe(filter((v) => v % 2 === 0)).pipe(map((value) => value.toFixed()));

  mapped.traversal.filter.events.filtered.listen((self, value) => {
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
  const source = new AsyncGenerator(async function* () {
    await new Promise((r) => setTimeout(r, 500));
    yield 1;
    await new Promise((r) => setTimeout(r, 500));
    yield 2;
    await new Promise((r) => setTimeout(r, 500));
    yield 3;
  });
  source
    .listen({
      handler: (self, value) => {
        console.log(value);
        self.next();
      },
    })
    .next();
  const streamA = new Stream({ source });
  const streamB = new Stream({ source });

  // streamA.listen((self, v) => {
  //   console.log("A:", v);
  //   self.next();
  // });

  // streamB.listen((self, v) => {
  //   console.log("B:", v);
  //   self.next();
  // });
}

fromIterableTest();
