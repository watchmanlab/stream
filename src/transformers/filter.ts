import { EventsLinker } from "../core/events-linker";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { NonEmptyString } from "../core/types";

export class Filter<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends NonEmptyString = Filter.Name,
> extends Transformer<INPUT, FILTERED, NAME> {
  protected override _eventsLinker: EventsLinker<Filter.Events<FILTERED>, NAME, this>;

  constructor(
    input: INPUT,
    predicate: Filter.Predicate<VALUE, FILTERED>,
    options?: Filter.Options<INPUT, VALUE, FILTERED, NAME>,
  ) {
    const { name = Filter.NAME as NAME, events, ...restOptions } = { ...options };

    super(input, {
      ...restOptions,
      name,
      source: {
        listen: (handler, options) => {
          return input.listen((self, value) => {
            if (predicate(value)) {
              handler(self, value);
            } else {
              _eventsLinker.emit("filtered", value);
              self.next();
            }
          }, options);
        },
      },
    });
    this._eventsLinker = new EventsLinker(this, events);
    const { _eventsLinker } = this;
  }
  override get events(): EventsLinker.EventStreams<Filter.Events<FILTERED>, NAME> {
    return this._eventsLinker.events;
  }
}

export function filter<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends NonEmptyString = Filter.Name,
>(
  predicate: Filter.Predicate<VALUE, FILTERED>,
  options?: Filter.Options<INPUT, VALUE, FILTERED, NAME>,
): Stream.Transform<INPUT, NAME, Filter<INPUT, VALUE, FILTERED, NAME>> {
  return (input, name) => new Filter(input, predicate, { ...options, name });
}

export namespace Filter {
  export const NAME = "filter";
  export type Name = typeof NAME;
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);

  export type Events<VALUE> = Stream.Events<VALUE> & { filtered: VALUE };
  export type Options<
    INPUT extends Stream.AnyStream,
    VALUE extends Stream.ExtractValue<INPUT>,
    FILTERED extends VALUE,
    NAME extends NonEmptyString,
  > = Omit<Transformer.Options<FILTERED, NAME>, "events" | "source"> & {
    events?: EventsLinker.EventsFunctions<Events<VALUE>, Filter<INPUT, VALUE, FILTERED, NAME>>;
  };
}
