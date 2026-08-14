import { Producer } from "../core/producer";

import { AnyProducer, ExtractValue, NonEmptyString, Consumable, Transform } from "../core/types";

export function filter<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends NonEmptyString = "$filter",
>(
  predicate: Filter.Predicate<VALUE, FILTERED>,
  options?: Filter.Options<VALUE, FILTERED, NAME>,
): Transform<INPUT, NAME, Producer<FILTERED, NAME> & { $rejected: Consumable<VALUE> }> {
  return (input) => {
    const { name, rejected, ...rest } = options ?? {};

    let $rejected: Producer<VALUE> | undefined;

    const output = new Producer({
      ...rest,
      name: name ?? ("$filter" as NAME),
      source: {
        consume() {
          return input.consume((consumer, value) => {
            if (predicate(value)) {
              output.push(value);
            } else {
              rejected?.(output, value);
              consumer.next();
            }
          });
        },
      },
    });
    return Object.defineProperty(output, "$rejected", {
      get() {
        return ($rejected ??= new Producer({ lastConsumerLeft: () => ($rejected = undefined) })).asSource();
      },
    }) as Producer<FILTERED, NAME> & { $rejected: Consumable<VALUE> };
  };
}

export namespace Filter {
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);

  export type Options<VALUE, FILTERED, NAME extends NonEmptyString> = Omit<
    Producer.Options<FILTERED, NAME>,
    "source"
  > & {
    rejected?: (stream: Producer<FILTERED, NAME>, value: VALUE) => void;
  };
}
