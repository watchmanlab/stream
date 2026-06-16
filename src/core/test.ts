import { map } from "./map";
import { Smoker } from "./smoker";

const smoker = new Smoker<number>();

smoker.pipe(map((v) => v * 2)).listen((v, consumer) => {
  console.log(v);
  consumer.next();
});

smoker.push(4);
smoker.push(5);
smoker.push(6);
