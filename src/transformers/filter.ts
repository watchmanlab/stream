import { EventsProxy } from "../core/events-proxy";
import { Stream, stream } from "../core/stream";
import { transformer, Transformer } from "../core/transformer";
import { Prettify } from "../core/types";

export class Filter<
  INPUT extends stream.AnyStream,
  VALUE extends stream.ExtractValue<INPUT> = stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
> extends Transformer<INPUT, FILTERED, NAME> {
  protected override _eventsProxy: EventsProxy<stream.Events<FILTERED> & { filtered: VALUE }, NAME, this>;

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
              _eventsProxy.emit("filtered", value);
              self.next();
            }
          }, options);
        },
      },
    });
    this._eventsProxy = new EventsProxy(name, this, hooks);
    const { _eventsProxy } = this;
  }
  override get events(): EventsProxy.EventsStream<stream.Events<FILTERED> & { filtered: VALUE }, NAME> {
    return this._eventsProxy.events;
  }
}

export function filter<
  INPUT extends stream.AnyStream,
  VALUE extends stream.ExtractValue<INPUT> = stream.ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
>(
  predicate: filter.Predicate<VALUE, FILTERED>,
  options?: filter.Options<INPUT, VALUE, FILTERED, NAME>,
): stream.Transform<INPUT, NAME, Filter<INPUT, VALUE, FILTERED, NAME>> {
  return (input, name) => new Filter(input, predicate, { ...options, name });
}

export namespace filter {
  export const NAME = "filter";
  export type Name = typeof NAME;
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);

  export type Events<VALUE> = stream.Events<VALUE> & { filtered: VALUE };
  export type Options<
    INPUT extends stream.AnyStream,
    VALUE extends stream.ExtractValue<INPUT>,
    FILTERED extends VALUE,
    NAME extends string,
  > = transformer.Options<FILTERED, NAME> & EventsProxy.Hooks<Events<VALUE>, Filter<INPUT, VALUE, FILTERED, NAME>>;
}
