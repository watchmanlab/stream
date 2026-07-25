import { EventsLinker } from "../core/events-linker";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, EventHandlers, EventStreams, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Filter<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends NonEmptyString = "filter",
> extends Transformer<INPUT, FILTERED, NAME> {
  protected override _eventsLinker: EventsLinker<Filter.Events<FILTERED>, NAME, this>;

  constructor(
    input: INPUT,
    predicate: Filter.Predicate<VALUE, FILTERED>,
    options?: Filter.Options<INPUT, VALUE, FILTERED, NAME>,
  ) {
    const { name = "filter" as NAME, events, ...restOptions } = { ...options };

    super(input, {
      ...restOptions,
      name,
      source: {
        consume: (handler, options) => {
          return input.consume((self, value) => {
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
  override get events(): EventStreams<Filter.Events<FILTERED>, NAME> {
    return this._eventsLinker.events;
  }
}

export function filter<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends NonEmptyString = "filter",
>(
  predicate: Filter.Predicate<VALUE, FILTERED>,
  options?: Filter.Options<INPUT, VALUE, FILTERED, NAME>,
): Transform<INPUT, NAME, Filter<INPUT, VALUE, FILTERED, NAME>> {
  return (input, name) => new Filter(input, predicate, { ...options, name: name ?? options?.name });
}

export namespace Filter {
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);

  export type Events<VALUE> = Stream.Events<VALUE> & { filtered: VALUE };
  export type Options<
    INPUT extends AnyStream,
    VALUE extends ExtractValue<INPUT>,
    FILTERED extends VALUE,
    NAME extends NonEmptyString,
  > = Omit<Transformer.Options<FILTERED, NAME>, "events" | "source"> & {
    events?: EventHandlers<Events<VALUE>, Filter<INPUT, VALUE, FILTERED, NAME>>;
  };
}
