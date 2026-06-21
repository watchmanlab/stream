import { Subject } from "rxjs";
import { map, filter } from "rxjs/operators";

function rxjsBench() {
  const MAX = 7_000_000;
  const subject = new Subject<number>();
  const start = performance.now();

  subject
    .pipe(
      map((v) => v * 2),
      filter((v) => v <= MAX * 2),
      filter((v) => v <= MAX * 2),
      filter((v) => v <= MAX * 2),
      filter((v) => v <= MAX * 2),
      filter((v) => v <= MAX * 2),
      filter((v) => v <= MAX * 2),
      filter((v) => v <= MAX * 2),
      filter((v) => v <= MAX * 2),
    )
    .subscribe((v) => {
      if (v === MAX) {
        console.log("RxJS:", Math.round(performance.now() - start), "ms");
      }
    });

  for (let i = 0; i <= MAX; i++) {
    subject.next(i);
  }
}

rxjsBench();
