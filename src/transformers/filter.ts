import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Filter<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends NonEmptyString = "$filter",
> extends Transformer<INPUT, FILTERED, NAME> {
  declare protected _options: Filter.Options<INPUT, VALUE, FILTERED, NAME>;

  constructor(
    input: INPUT,
    predicate: Filter.Predicate<VALUE, FILTERED>,
    options?: Filter.Options<INPUT, VALUE, FILTERED, NAME>,
  ) {
    const inputConsumer = input.consume((self, value) => {
      if (predicate(value)) {
        this.push(value);
      } else {
        this._options.rejected?.(this, value);
        this._options.$rejected?.push(value);
        self.next();
      }
    });
    super(input, {
      ...options,
      name: options?.name ?? ("$filter" as NAME),
      next: (self, consumer) => {
        options?.next?.(self, consumer);
        inputConsumer.next();
      },
      terminate: (self, reason) => {
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
      },
    });
  }
  get $rejected(): Stream<VALUE, `${NAME}Rejected`> {
    this._options.$rejected ??= new Stream({ scope: [this] });
    return new Stream({ name: `${this.name}Rejected`, source: this._options.$rejected });
  }
}

export function filter<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends NonEmptyString = "$filter",
>(
  predicate: Filter.Predicate<VALUE, FILTERED>,
  options?: Filter.Options<INPUT, VALUE, FILTERED, NAME>,
): Transform<INPUT, Filter<INPUT, VALUE, FILTERED, NAME>> {
  return (input) => new Filter(input, predicate, options);
}

export namespace Filter {
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);

  export type Options<
    INPUT extends AnyStream,
    VALUE extends ExtractValue<INPUT>,
    FILTERED extends VALUE,
    NAME extends NonEmptyString,
  > = Stream.Options<FILTERED, NAME> & {
    rejected?: (self: Filter<INPUT, VALUE, FILTERED, NAME>, value: FILTERED) => void;
    $rejected?: Stream<VALUE, any>;
  };
}
