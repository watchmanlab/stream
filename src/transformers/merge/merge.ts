import { Stream } from "../../streams";
import { each } from "../each";

const NAME = "merge";

export class Merge<
  SOURCE extends Stream<any, any>,
  VALUE = Stream.ExtractValue<SOURCE>,
  ITERABLES extends [AsyncIterable<any>, ...AsyncIterable<any>[]] = [AsyncIterable<any>],
  NAME extends string = merge.Name,
> extends Stream<VALUE | Stream.ExtractValue<ITERABLES[number]>, NAME> {
  constructor(source: SOURCE, name = NAME as NAME, others: ITERABLES) {
    super(name, async function* () {
      const iters = [source, ...others].map((i) => i[Symbol.asyncIterator]());

      const active = new Map(iters.map((it, i) => [i, { it, next: it.next() }]));

      try {
        while (active.size > 0) {
          const results = await Promise.race(
            Array.from(active.entries()).map(([id, { next }]) => next.then((result) => ({ result, id }))),
          );

          const { result, id } = results;
          const entry = active.get(id)!;

          if (result.done) {
            active.delete(id);
            continue;
          }

          yield result.value;
          entry.next = entry.it.next();
        }
      } finally {
        await Promise.all(iters.map((iter) => iter.return?.()));
      }
    });
  }
}

export function merge<
  SOURCE extends Stream<any, any>,
  VALUE = Stream.ExtractValue<SOURCE>,
  ITERABLES extends [AsyncIterable<any>, ...AsyncIterable<any>[]] = [AsyncIterable<any>],
  NAME extends string = merge.Name,
>(...others: ITERABLES): Stream.Transformer<NAME, SOURCE, Merge<SOURCE, VALUE, ITERABLES, NAME>> {
  return (_, source, name) => new Merge(source, name, others);
}

export namespace merge {
  export type Name = typeof NAME;
}
