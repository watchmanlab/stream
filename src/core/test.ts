import { map } from "./map";
import { Stream } from "./stream";

const stream = new Stream<number>();

const mapped = stream.pipe(map((v) => v * 2));
mapped.listen((v, self) => {
  console.log(v);
  self.next();
});

stream.push(4);
stream.push(5);
stream.push(6);
