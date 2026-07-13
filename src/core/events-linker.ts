import { Stream } from "./stream";
import { AnyStream, Closable, EventHandlers, EventStreams, Named, NonEmptyString } from "./types";

export class EventsLinker<
  EVENTS extends Record<string, unknown>,
  NAME extends NonEmptyString,
  SELF extends Named,
> implements Closable {
  protected _events: Partial<EventStreams<EVENTS, NAME>> = {};
  constructor(
    private self: SELF,
    functions?: EventHandlers<EVENTS, SELF>,
  ) {
    if (functions) {
      this.emit = (eventName, value) => {
        functions[eventName]?.(self, value);
        this._events[eventName]?.push?.(value);
      };
    } else {
      this.emit = (eventName, value) => {
        this._events[eventName]?.push?.(value);
      };
    }
  }

  emit<KEY extends keyof EVENTS, VALUE extends EVENTS[KEY]>(eventName: KEY, value: VALUE): void {}
  has<KEY extends keyof EVENTS>(eventName: KEY): boolean {
    return this._events[eventName] !== undefined;
  }
  get events(): EventStreams<EVENTS, NAME> {
    const { _events } = this;

    return new Proxy(_events as EventStreams<EVENTS, NAME>, {
      get: (target, p: string, receiver) => {
        if (p in target) return Reflect.get(target, p, receiver);
        const stream = new Stream({
          name: (this.self.name + p[0].toUpperCase() + p.slice(1)) as NonEmptyString,
          events: {
            consumerLeft(self) {
              if (self.infos.consumersCount === 0) delete (_events as any)[p];
            },
          },
        });
        (_events as any)[p] = stream;
        return stream;
      },
    });
  }
  abort() {
    for (const event of Object.values(this._events)) (event as AnyStream).abort();
  }
  complete() {
    for (const event of Object.values(this._events)) (event as AnyStream).complete();
  }
}

export namespace EventsLinker {}
