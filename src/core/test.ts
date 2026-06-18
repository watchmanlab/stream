import { map } from "./map";
import { Stream } from "./stream";

const stream = new Stream<number>();

const consumer = stream.pipe(map((v) => v * 2)).listen(
  (v, consumer) => {
    console.log(v);
    consumer.next();
  },
  { isReady: true },
);

stream.push(4);
stream.push(5);
stream.push(6);

console.log([...consumer.queue]); // []
