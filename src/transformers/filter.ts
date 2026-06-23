import { Stream, stream } from "../core/stream";
import { Transformer } from "../core/transformer";

export class Filter<
  INPUT extends stream.AnyStream,
  VALUE extends stream.ExtractValue<INPUT> = stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
> extends Transformer<INPUT, FILTERED, NAME> {
  protected override _events?: Partial<stream.Events<FILTERED, NAME> & { filtered: Stream<VALUE, `${NAME}Filtered`> }>;

  constructor(
    name = filter.NAME as NAME,
    input: INPUT,
    public readonly predicate: filter.Predicate<VALUE, FILTERED>,
  ) {
    super(name, input, {
      source: {
        listen: (init) => {
          return input.listen({
            ...init,
            handler: (self, value) => {
              if (predicate(value)) {
                init.handler(self, value);
              } else {
                this._events?.filtered?.push(value);
                self.next();
              }
            },
          });
        },
      },
    });
  }

  override get events(): stream.Events<FILTERED, NAME> & { filtered: Stream<VALUE, `${NAME}Filtered`> } {
    return super.events as never;
  }
}

export function filter<
  INPUT extends stream.AnyStream,
  VALUE extends stream.ExtractValue<INPUT> = stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
>(predicate: filter.Predicate<VALUE, FILTERED>): stream.Transform<INPUT, NAME, Filter<INPUT, VALUE, FILTERED, NAME>> {
  return (input, name) => new Filter(name, input, predicate);
}

export namespace filter {
  export const NAME = "filter";
  export type Name = typeof NAME;
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);
}
