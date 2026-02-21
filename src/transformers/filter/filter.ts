import { Stream } from "../../streams/";

const NAME = "filter";

export class Filter<VALUE, FILTERED extends VALUE = VALUE, NAME extends string = Filter.Name> extends Stream<
  FILTERED,
  NAME
> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, predicate: Filter.Predicate<VALUE>) {
    super(name, async function* () {
      for await (const value of source) {
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
  return (_, source, name) => new Filter(source, name, predicate);
}

export namespace Filter {
  export type Name = typeof NAME;
  export type GardPredicate<VALUE, FILTERED extends VALUE = VALUE> = (value: VALUE) => value is FILTERED;
  export type Predicate<VALUE> = (value: VALUE) => boolean | Promise<boolean>;
}
