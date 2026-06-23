import { Stream, stream } from "../core/stream";
import { transformer, Transformer } from "../core/transformer";
import { Prettify } from "../core/types";

export class Filter<
  INPUT extends stream.AnyStream,
  VALUE extends stream.ExtractValue<INPUT> = stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
> extends Transformer<INPUT, FILTERED, NAME> {
  protected override _events?: Partial<stream.Events<FILTERED, NAME> & { filtered: Stream<VALUE, `${NAME}Filtered`> }>;

  constructor(input: INPUT, predicate: filter.Predicate<VALUE, FILTERED>, options?: filter.Options<FILTERED, NAME>) {
    super(input, {
      ...options,
      name: options?.name ?? (filter.NAME as NAME),
      source: {
        listen: (handler, options) => {
          return input.listen((self, value) => {
            if (predicate(value)) {
              handler(self, value);
            } else {
              this._events?.filtered?.push(value);
              self.next();
            }
          }, options);
        },
      },
    });
  }

  override get events(): Prettify<stream.Events<FILTERED, NAME> & { filtered: Stream<VALUE, `${NAME}Filtered`> }> {
    return super.events as never;
  }
}

export function filter<
  INPUT extends stream.AnyStream,
  VALUE extends stream.ExtractValue<INPUT> = stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
>(
  predicate: filter.Predicate<VALUE, FILTERED>,
  options?: filter.Options<FILTERED, NAME>,
): stream.Transform<INPUT, NAME, Filter<INPUT, VALUE, FILTERED, NAME>> {
  return (input, name) => new Filter(input, predicate, { ...options, name });
}

export namespace filter {
  export const NAME = "filter";
  export type Name = typeof NAME;
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);

  export type Options<FILTERED, NAME extends string> = transformer.Options<FILTERED, NAME>;
}
