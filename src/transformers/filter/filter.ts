import { Stream } from "../../stream";
import { map } from "../map";

export class Filter<VALUE, FILTERED extends VALUE = VALUE> extends Stream<FILTERED, "filtered"> {
  constructor(
    source: Stream<VALUE, any>,
    private predicate: (value: VALUE) => boolean | Promise<boolean>,
  ) {
    super(async function* () {
      for await (const value of source) {
        if (await predicate(value)) yield value as FILTERED;
      }
    }, "filtered");
  }
}

export function filter<VALUE, FILTERED extends VALUE = VALUE>(
  predicate: filter.GardPredicate<VALUE, FILTERED>,
): Stream.Transformer<Stream<VALUE, any>, Filter<VALUE, FILTERED>>;

export function filter<VALUE>(
  predicate: filter.Predicate<VALUE>,
): Stream.Transformer<Stream<VALUE, any>, Filter<VALUE>>;

export function filter<VALUE>(
  predicate: filter.Predicate<VALUE>,
): Stream.Transformer<Stream<VALUE, any>, Filter<VALUE>> {
  return (source: Stream<VALUE, any>) => new Filter<VALUE>(source, predicate);
}

export namespace filter {
  export type GardPredicate<VALUE, FILTERED extends VALUE = VALUE> = (value: VALUE) => value is FILTERED;
  export type Predicate<VALUE> = (value: VALUE) => boolean | Promise<boolean>;
}

const stream = new Stream<number, "user">([], "user").pipe(filter((x) => x > 0)).pipe(map((x) => x.toFixed()));

stream.listen((v) => console.log(v));
