import { Stream } from "../../stream";

export const filter: filter.Function = <VALUE, FILTERED extends VALUE = VALUE>(
  predicate: filter.Predicate<VALUE>,
): Stream.Transformer<Stream<VALUE>, Stream<FILTERED>> => {
  return (source) => {
    return new Stream<FILTERED>(async function* () {
      for await (const value of source) {
        if (predicate(value)) yield value as FILTERED;
      }
    });
  };
};

export namespace filter {
  export type GardPredicate<VALUE, FILTERED extends VALUE = VALUE> = (value: VALUE) => value is FILTERED;
  export type Predicate<VALUE> = (value: VALUE) => boolean;

  export interface Function {
    <VALUE, FILTERED extends VALUE = VALUE>(
      predicate: GardPredicate<VALUE, FILTERED>,
    ): Stream.Transformer<Stream<VALUE>, Stream<FILTERED>>;

    <VALUE>(predicate: Predicate<VALUE>): Stream.Transformer<Stream<VALUE>, Stream<VALUE>>;
  }
}
