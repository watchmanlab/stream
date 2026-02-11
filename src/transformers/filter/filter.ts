import { Stream } from "../../stream";
import { snapshot } from "../snapshot";

export function filter<SOURCE extends Stream<any>, FILTERED extends Stream.ValueOf<SOURCE> = Stream.ValueOf<SOURCE>>(
  predicate: filter.GardPredicate<SOURCE, FILTERED>,
): Stream.Transformer<SOURCE, Filter<SOURCE, FILTERED>>;
export function filter<SOURCE extends Stream<any>>(
  predicate: filter.Predicate<SOURCE>,
): Stream.Transformer<SOURCE, Filter<SOURCE, Stream.ValueOf<SOURCE>>>;

export function filter<SOURCE extends Stream<any>>(
  predicate: filter.Predicate<Stream.ValueOf<SOURCE>>,
): Stream.Transformer<SOURCE, Filter<SOURCE, Stream.ValueOf<SOURCE>>> {
  return (source) =>
    new Filter(source, async function* () {
      for await (const value of source) {
        if (await predicate(value)) yield value;
      }
    });
}
class Filter<SOURCE extends Stream<any>, VALUE> extends Stream<VALUE> {
  constructor(
    public readonly source: SOURCE,
    fn: Stream.GeneratorFunction<VALUE>,
  ) {
    super(fn);
  }
  get name(): "filter" {
    return "filter";
  }
}

export namespace filter {
  export type GardPredicate<
    SOURCE extends Stream<any>,
    FILTERED extends Stream.ValueOf<SOURCE> = Stream.ValueOf<SOURCE>,
  > = (value: Stream.ValueOf<SOURCE>) => value is FILTERED;
  export type Predicate<SOURCE extends Stream<any>> = (value: Stream.ValueOf<SOURCE>) => boolean | Promise<boolean>;
}

const stream = new Stream<number>().pipe(filter((x) => x > 0)).pipe(filter((x) => x > 0));
// .pipe(snapshot("f2"));

stream.listen((v) => console.log(v));
