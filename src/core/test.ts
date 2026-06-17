import { map } from "./map";
import { Smoker } from "./smoker";

const smoker = new Smoker<number>();

const consumer = smoker.pipe(map((v) => v * 2)).listen(
  (v, consumer) => {
    console.log(v);
    consumer.next();
  },
  { isReady: true },
);

smoker.push(4);
smoker.push(5);
smoker.push(6);

console.log([...consumer.get("queue")]); // []
