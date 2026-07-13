import { EventsLinker } from "../core/events-linker";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyStream, EventHandlers, EventStreams, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Resolve<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
  NAME extends NonEmptyString = "resolve",
> extends Transformer<INPUT, VALUE, NAME> {
  protected override _eventsLinker: EventsLinker<Resolve.Events<VALUE>, NAME, this>;
  constructor(input: INPUT, concurrency = 1, options?: Resolve.Options<INPUT, VALUE, NAME>) {
    const { name = "resolve" as NAME, events, ...restOptions } = { ...options };
    super(input, {
      ...restOptions,
      name,
      source: {
        listen: (handler, options) => {
          let count = 0;
          return input.listen((self, maybePromise) => {
            if (++count < concurrency) self.next();

            if (maybePromise instanceof Promise) {
              maybePromise
                .then((value) => handler(self, value))
                .catch((error) => {
                  this._eventsLinker.emit("error", error);
                  self.next();
                })
                .finally(() => {
                  count--;
                });
            } else {
              handler(self, maybePromise);
              count--;
            }
          }, options);
        },
      },
    });

    this._eventsLinker = new EventsLinker(this, events);
  }

  override get events(): EventStreams<Resolve.Events<VALUE>, NAME> {
    return this._eventsLinker.events;
  }
}

export function resolve<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
  NAME extends NonEmptyString = "resolve",
>(concurrency = 1, options?: Resolve.Options<INPUT, VALUE, NAME>): Transform<INPUT, NAME, Resolve<INPUT, VALUE, NAME>> {
  return (input, name) => new Resolve(input, concurrency, { ...options, name: name ?? options?.name });
}

export namespace Resolve {
  export type Events<VALUE> = Stream.Events<VALUE> & { error: any };
  export type Options<
    INPUT extends AnyStream,
    VALUE extends ExtractValue<ExtractValue<INPUT>>,
    NAME extends NonEmptyString,
  > = Omit<Transformer.Options<VALUE, NAME>, "source" | "events"> & {
    events?: EventHandlers<Events<VALUE>, Resolve<INPUT, VALUE, NAME>>;
  };
}
