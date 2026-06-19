import { map } from "./map";
import { Stream } from "./stream";

const stream = new Stream<number>();
const mapped = stream.pipe(map((v) => v * 2));

mapped.event.listen((self, e) => {
  if (e.type === "error") console.log(e.error);
});
mapped.listen((self, v) => {
  // if (v == 12) self.abort();

  if (v === 10) {
    queueMicrotask(() => {
      console.log(v);
      self.next();
    });
    return;
  }
  console.log(v);

  self.next();
});

stream.push(4);
stream.push(5);
stream.push(6);
stream.push(7);

//

function bench() {
  const MAX = 10_000_000;
  const stream = new Stream<number>();

  const start = performance.now();
  stream.pipe(map((v) => v++)).listen((self, value) => {
    if (value === MAX) console.log("moo", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");

    self.next();
  });

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
}

// bench();
