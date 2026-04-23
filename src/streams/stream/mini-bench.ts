import { map } from "./map";
import { Stream } from "./stream";

function benchTransform() {
  const stream = new Stream<number>();
  const mapped = stream
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(
      "map3",
      map((v) => v),
    )
    .pipe(map((v) => v));

  const now = performance.now();
  const MAX = 1_000_000;
  {
    using abort = mapped.listen((v) => {
      if (v === MAX) {
        console.log("hot x 10", Math.round(performance.now() - now));
      }
    });

    // (async () => {
    //   for await (const v of mapped) {
    //     if (v === MAX) console.log("cold x 10", Math.round(performance.now() - now));
    //   }
    // })();
    for (let i = 1; i <= MAX; i++) {
      stream.push(i);
    }
  }

  console.log("listenersCount", mapped.consumersCount);
  console.log("firstListenerAdded-listenersCount", mapped.firstConsumerAdded.consumersCount);
  console.log("lastListenerRemoved-listenersCount", mapped.lastConsumerRemoved.consumersCount);
}
function benchStreamCore() {
  const MAX = 1_000_000;
  const now = performance.now();
  const stream = new Stream<number>();
  stream.valueDropped.listen((value) => {
    if (value === MAX) console.log("hot dropped", Math.round(performance.now() - now));
  });
  using abort = stream.listen((value) => {
    if (value === MAX) console.log("hot", Math.round(performance.now() - now));
    return value * 10;
  });

  // (async () => {
  //   for await (const value of stream) {
  //     if (value === MAX) console.log("cold", Math.round(performance.now() - now));
  //   }
  // })();

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}

benchTransform();
// benchStreamCore();
