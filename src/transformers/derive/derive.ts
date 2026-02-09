import { Stream } from "../../stream-0";

export function derive<VALUE>(): Stream.Transformer<Stream<VALUE>> {
  return function (source) {
    return new Stream((self) => {
      return source.listen((v) => self.push(v));
    });
  };
}
