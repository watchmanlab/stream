import { Stream } from "../../stream";

export function merge<VALUE, ITERABLES extends [AsyncIterable<any>, ...AsyncIterable<any>[]]>(
  ...other: ITERABLES
): Stream.Transformer<Stream<VALUE>, Stream<VALUE | Stream.ValueOf<ITERABLES[number]>>> {
  return (source) =>
    new Stream(async function* () {
      const iters = [source, ...other].map((i) => i[Symbol.asyncIterator]());

      const nexts = iters.map((it, index) => it.next().then((res) => ({ res, index })));

      try {
        while (nexts.length > 0) {
          const { res, index } = await Promise.race(nexts);

          if (res.done) {
            nexts.splice(index, 1);
            iters.splice(index, 1);
            continue;
          }

          yield res.value;

          nexts[index] = iters[index].next().then((r) => ({ res: r, index }));
        }
      } finally {
        for (const it of iters) it.return?.();
      }
    });
}
