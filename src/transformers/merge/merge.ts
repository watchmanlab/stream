import { Stream } from "../../stream";

const NAME = "merged";
type Name = typeof NAME;

class Merge<
  VALUE,
  ITERABLES extends [AsyncIterable<any>, ...AsyncIterable<any>[]],
  NAME extends string = Name,
> extends Stream<VALUE | Stream.ValueOf<ITERABLES[number]>, NAME> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, others: ITERABLES) {
    super(name, async function* () {
      const iters = [source, ...others].map((i) => i[Symbol.asyncIterator]());

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
}

export function merge<
  VALUE,
  ITERABLES extends [AsyncIterable<any>, ...AsyncIterable<any>[]],
  NAME extends string = Name,
>(...others: ITERABLES): Stream.Transformer<NAME, Stream<VALUE>, Merge<VALUE, ITERABLES, NAME>> {
  return (_, source, name) => new Merge(source, name, others);
}
