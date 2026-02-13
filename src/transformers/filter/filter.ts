import { Stream } from "../../stream";
import { map } from "../map";

export class Filter<VALUE, FILTERED extends VALUE = VALUE, NAME extends string = "filtered"> extends Stream<
  FILTERED,
  NAME
> {
  constructor(
    source: Stream<VALUE, any>,
    name = "filtered" as NAME,
    private predicate: (value: VALUE) => boolean | Promise<boolean>,
  ) {
    super(async function* () {
      for await (const value of Stream.generator(source)) {
        if (await predicate(value)) yield value as FILTERED;
      }
    }, name);
  }
}

export function filter<VALUE, FILTERED extends VALUE = VALUE, NAME extends string = "filtered">(
  predicate: filter.GardPredicate<VALUE, FILTERED>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, FILTERED, NAME>>;

export function filter<VALUE, NAME extends string = "filtered">(
  predicate: filter.Predicate<VALUE>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, VALUE, NAME>>;

export function filter<VALUE, NAME extends string = "filtered">(
  predicate: filter.Predicate<VALUE>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, VALUE, NAME>> {
  return (source: Stream<VALUE, any>, name?: NAME) => new Filter<VALUE, VALUE, NAME>(source, name, predicate);
}

export namespace filter {
  export type GardPredicate<VALUE, FILTERED extends VALUE = VALUE> = (value: VALUE) => value is FILTERED;
  export type Predicate<VALUE> = (value: VALUE) => boolean | Promise<boolean>;
}

const stream = new Stream<number, "user">([], "user")
  .pipe(
    filter((x) => x > 0),
    "validated",
  )
  .pipe(
    map((x) => x.toFixed()),
    "toFixed",
  );

stream.listen((v) => console.log(v));
