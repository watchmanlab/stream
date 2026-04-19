import { map } from "./map";
import { Stream } from "./stream";

function benchTransform() {
  const stream = new Stream<number>();
  const mapped = stream
    .pipe(
      "map1",
      map((v) => v),
    )
    .pipe(
      "map2",
      map((v) => v),
    )

    .pipe(
      map((v) => {
        // if (v === 4) return Stream.err("kechmahaja" as const);
        return v;
      }),
    );

  const now = performance.now();
  const MAX = 1_000_000;
  // mapped.listen((v) => {
  //   if (v === MAX) console.log("hot", performance.now() - now);
  // });
  // mapped.error.listen((err) => {
  //   console.log(err);
  // });

  (async () => {
    for await (const v of mapped) {
      if (v === MAX) console.log("cold", performance.now() - now);
    }
  })();
  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}
function benchStreamCore() {
  const stream = new Stream<number>([4, 5, 6]);
  //   stream.listen(console.log);
  (async () => {
    for await (const value of stream) {
      console.log(value);
    }
  })();
  //   stream.push(1, 2, 3);
}

// benchTransform();
benchStreamCore();
