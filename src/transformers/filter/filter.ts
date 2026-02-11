import { Stream } from "../../stream";

export function filter<VALUE, FILTERED extends VALUE = VALUE>(
  predicate: filter.GardPredicate<VALUE, FILTERED>,
): Stream.Transformer<"filter", Stream<VALUE>, Stream<FILTERED>>;
export function filter<VALUE>(predicate: filter.Predicate<VALUE>): Stream.Transformer<"filter", Stream<VALUE>>;

export function filter<VALUE>(predicate: filter.Predicate<VALUE>) {
  return Stream.createTransformer("filter", (source) => {
    return new Stream<VALUE>(async function* () {
      for await (const value of source) {
        if (predicate(value)) yield value;
      }
    });
  });
}

export namespace filter {
  export type GardPredicate<VALUE, FILTERED extends VALUE = VALUE> = (value: VALUE) => value is FILTERED;
  export type Predicate<VALUE> = (value: VALUE) => boolean;
}

const stream = new Stream<number>()
  .pipe(
    filter((x) => x > 0),
    "f1",
  )
  .pipe(
    filter((x) => x > 0),
    "f2",
  );

stream.listen((v) => console.log(v));
