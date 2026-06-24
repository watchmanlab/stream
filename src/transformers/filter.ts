import { EventsLinker } from "../core/events-linker";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { EventsFunctions, EventsStreams } from "../core/types";

export class Filter<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
> extends Transformer<INPUT, FILTERED, NAME> {
  protected override _eventsLinker: EventsLinker<filter.Events<FILTERED>, NAME, this>;

  constructor(
    input: INPUT,
    predicate: filter.Predicate<VALUE, FILTERED>,
    options?: filter.Options<INPUT, VALUE, FILTERED, NAME>,
  ) {
    const { name = filter.NAME as NAME, scope, source, queueFactory, ...hooks } = { ...options };

    super(input, {
      ...options,
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
    this._eventsLinker = new EventsLinker(this, hooks);
    const { _eventsLinker } = this;
  }
  override get events(): EventsStreams<filter.Events<FILTERED>, NAME> {
    return this._eventsLinker.events;
  }
}

export function filter<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
>(
  predicate: filter.Predicate<VALUE, FILTERED>,
  options?: filter.Options<INPUT, VALUE, FILTERED, NAME>,
): Stream.Transform<INPUT, NAME, Filter<INPUT, VALUE, FILTERED, NAME>> {
  return (input, name) => new Filter(input, predicate, { ...options, name });
}

export namespace filter {
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
    NAME extends string,
  > = Transformer.Options<FILTERED, NAME> & EventsFunctions<Events<VALUE>, Filter<INPUT, VALUE, FILTERED, NAME>>;
}
