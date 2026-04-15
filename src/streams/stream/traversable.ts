import { Stream } from "./stream";

export function traversable<INPUT_STREAM extends Stream.AnyStream, PARENT extends Stream.AnyStream>(
  parent: PARENT,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<INPUT_STREAM, PARENT>> {
  return (inputStream) =>
    new Proxy(inputStream, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return parent;
      },
    }) as never;
}

const p = new Stream<string>();

const d = new Stream<number>().pipe(traversable(p));

d.stream.listen(console.log);

p.push("1", "3");
