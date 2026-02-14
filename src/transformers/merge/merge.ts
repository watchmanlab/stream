import { Stream } from "../../stream";

export class Merge<
  VALUE,
  ITERABLES extends [AsyncIterable<any>, ...AsyncIterable<any>[]],
  NAME extends string = Merge.Name,
> extends Stream<VALUE | Stream.ValueOf<ITERABLES[number]>, NAME> {
  constructor(source: Stream<VALUE, any>, others: ITERABLES, options?: Merge.Options<NAME>) {
    const { name = Merge.NAME as NAME } = options ?? {};

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
  NAME extends string = Merge.Name,
>(...other: ITERABLES): Stream.Transformer<NAME, Stream<VALUE>, Merge<VALUE, ITERABLES, NAME>> {
  return (source, name) => new Merge(source, other, { name });
}

export namespace Merge {
  export const NAME = "merged";
  export type Name = typeof NAME;

  export type Options<NAME extends string> = {
    name?: NAME;
  };
}
