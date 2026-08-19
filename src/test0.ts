import { Consumer } from "./core/consumer0";
import { Stream } from "./core/stream0";

function consumer0Bench() {
  const MAX = 200_000_000;

  const start = performance.now();
  // Create the consumer data struct
  const consumer = Consumer.create<number>((consumer, v) => {
    if (v === MAX) {
      console.log("functional", v.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      // Consumer.terminate(consumer, "complete");
      // return;
    }

    Consumer.next(consumer);
  });

  Consumer.next(consumer);

  const pushValue = (value: number) => {
    Consumer.push(consumer, value);
  };

  for (let i = 0; i <= MAX; i++) {
    pushValue(i);
    // push(consumer, i);
  }
}

// consumer0Bench(); //functional 200 000 000 471 ms

function streamTest() {
  const $stream = Stream.create<number>();

  const consumer$ = Stream.consume($stream, (c, v) => {
    console.log(v);
    Consumer.next(c);
  });

  Consumer.next(consumer$);

  Stream.push($stream, 1);
  Stream.push($stream, 2);
  Stream.push($stream, 3);
}
// streamTest();
