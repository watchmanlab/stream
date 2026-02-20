import { Stream } from "../../streams/stream/stream-0";

export class Filter<VALUE, FILTERED extends VALUE = VALUE, NAME extends string = Filter.Name> extends Stream<
  FILTERED,
  NAME
> {
  constructor(
    source: Stream<VALUE, any>,

    private predicate: Filter.Predicate<VALUE>,
    options?: Filter.Options<NAME>,
  ) {
    const { name = Filter.NAME as NAME } = options ?? {};

    super(name, async function* () {
      for await (const value of Stream.generator(source)) {
        if (await predicate(value)) yield value as FILTERED;
      }
    });
  }
}

export function filter<VALUE, FILTERED extends VALUE = VALUE, NAME extends string = Filter.Name>(
  predicate: Filter.GardPredicate<VALUE, FILTERED>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, FILTERED, NAME>>;

export function filter<VALUE, NAME extends string = Filter.Name>(
  predicate: Filter.Predicate<VALUE>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, VALUE, NAME>>;

export function filter<VALUE, NAME extends string = Filter.Name>(
  predicate: Filter.Predicate<VALUE>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, VALUE, NAME>> {
  return (source, name) => new Filter(source, predicate, { name });
}

export namespace Filter {
  export const NAME = "filtered";
  export type Name = typeof NAME;
  export type Options<NAME extends string> = {
    name?: NAME;
  };
  export type GardPredicate<VALUE, FILTERED extends VALUE = VALUE> = (value: VALUE) => value is FILTERED;
  export type Predicate<VALUE> = (value: VALUE) => boolean | Promise<boolean>;
}
