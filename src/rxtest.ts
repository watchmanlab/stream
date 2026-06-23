import { Subject, asyncScheduler } from "rxjs";
import { map, filter, share, observeOn } from "rxjs/operators";

const v = filter(() => true)(new Subject<number>());

function rxjsBench() {
  const MAX = 7_000_000;
  const subject = new Subject<number>();
  const start = performance.now();

  const s = subject.pipe(
    map((v) => v * 2),
    filter((v) => v <= MAX * 2),
    filter((v) => v <= MAX * 2),
    // share(),
  );
  s.subscribe((v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), "RxJS:", Math.round(performance.now() - start), "ms");
    }
  });
  s.subscribe((v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), "RxJS:", Math.round(performance.now() - start), "ms");
    }
  });
  s.subscribe((v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), "RxJS:", Math.round(performance.now() - start), "ms");
    }
  });
  s.subscribe((v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), "RxJS:", Math.round(performance.now() - start), "ms");
    }
  });
  s.subscribe((v) => {
    if (v === MAX) {
      console.log(v.toLocaleString("fr"), "RxJS:", Math.round(performance.now() - start), "ms");
    }
  });

  for (let i = 0; i <= MAX; i++) {
    subject.next(i);
  }
}

rxjsBench();
// 7 000 000 RxJS: 556 ms
// 7 000 000 RxJS: 556 ms
// 7 000 000 RxJS: 556 ms
// 7 000 000 RxJS: 556 ms
// 7 000 000 RxJS: 556 ms
