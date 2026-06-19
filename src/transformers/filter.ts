import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { EventShape } from "../core/types";

export class Filter<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
> extends Transformer<INPUT, FILTERED, NAME> {
  protected override _events?: Partial<Stream.Events<FILTERED, NAME> & { filtered: Stream<VALUE, `${NAME}Filtered`> }>;

  constructor(
    name = filter.NAME as NAME,
    input: INPUT,
    public readonly predicate: filter.Predicate<VALUE, FILTERED>,
  ) {
    super(name, input, {
      source: {
        listen: (init) => {
          const inputConsumer = input.listen((self, value) => {
            if (predicate(value)) {
              outputConsumer.push(value);
            } else {
              this._events?.filtered?.push(value);
              self.next();
            }
          });

          const outputConsumer = new Consumer<FILTERED, any>({
            ...init,
            ready: (self) => {
              inputConsumer.next();
              init.ready?.(self);
            },
          });
          return outputConsumer;
        },
      },
    });
  }

  override get events(): Stream.Events<FILTERED, NAME> & { filtered: Stream<VALUE, `${NAME}Filtered`> } {
    return super.events as never;
  }
}

export function filter<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
>(predicate: filter.Predicate<VALUE, FILTERED>): Stream.Transform<INPUT, NAME, Filter<INPUT, VALUE, FILTERED, NAME>> {
  return (input, name) => new Filter(name, input, predicate);
}

export namespace filter {
  export const NAME = "filter";
  export type Name = typeof NAME;
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);
}
