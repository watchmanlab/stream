import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Filter<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends NonEmptyString = "$filter",
> extends Transformer<INPUT, FILTERED, NAME> {
  declare protected _options: Filter.Options<VALUE, FILTERED, NAME>;
  declare protected _metaStreams: Filter.MetaStreams<VALUE, FILTERED>;
  constructor(
    input: INPUT,
    predicate: Filter.Predicate<VALUE, FILTERED>,
    options?: Filter.Options<VALUE, FILTERED, NAME>,
  ) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((self, value) => {
      if (predicate(value)) {
        this.push(value);
      } else {
        this._options.rejected?.(this, value);
        this._metaStreams.$rejected?.push(value);
        self.next();
      }
    });
    super(input, {
      ...rest,
      name: name ?? ("$filter" as NAME),
      next: (self, consumer) => {
        inputConsumer.next();
        next?.(self, consumer);
      },
      terminate: (self, reason) => {
        inputConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });
  }
  get $rejected(): Stream<VALUE, `${NAME}Rejected`> {
    this._metaStreams.$rejected ??= new Stream({ $terminate: this.$terminate });
    return new Stream({ name: `${this.name}Rejected`, source: this._metaStreams.$rejected });
  }
}

export function filter<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends NonEmptyString = "$filter",
>(
  predicate: Filter.Predicate<VALUE, FILTERED>,
  options?: Filter.Options<VALUE, FILTERED, NAME>,
): Transform<INPUT, Filter<INPUT, VALUE, FILTERED, NAME>> {
  return (input) => new Filter(input, predicate, options);
}

export namespace Filter {
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);

  export type Options<VALUE, FILTERED, NAME extends NonEmptyString> = Stream.Options<FILTERED, NAME> & {
    rejected?: (self: Stream<FILTERED, NAME>, value: VALUE) => void;
  };

  export type MetaStreams<VALUE, FILTERED> = Stream.MetaStreams<FILTERED> & { $rejected?: Stream<VALUE, any> };
}
