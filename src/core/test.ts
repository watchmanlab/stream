import { map } from "./map";
import { Stream } from "./stream";

const stream = new Stream<number>();
const mapped = stream.pipe(map((v) => v * 2));

mapped.event.listen((self, e) => {
  if (e.type === "error") console.log(e.error);
});
mapped.listen((self, v) => {
  if (v == 12) self.abort();
  console.log(v);

  self.next();
});

stream.push(4);
stream.push(5);
stream.push(6);

//
