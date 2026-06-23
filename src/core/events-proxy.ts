import { Stream } from "./stream";
import { Evented } from "./types";

export class EventsProxy<
  EVENTS extends Record<string, unknown>,
  NAME extends string,
  SELF extends Evented<any> | undefined = undefined,
> {
  protected _events?: Partial<EventsProxy.EventsStream<EVENTS, NAME>>;
  constructor(
    private name: NAME,
    hooks?: EventsProxy.Hooks<EVENTS, SELF>,
  ) {
    if (hooks) {
      this.push = (eventName, value) => {
        hooks.handlers[eventName](hooks.self, value);
        this._events?.[eventName]?.push?.(value);
      };
    } else {
      this.push = (eventName, value) => {
        this._events?.[eventName]?.push?.(value);
      };
    }
  }

  push<KEY extends keyof EVENTS, VALUE extends EVENTS[KEY]>(eventName: KEY, value: VALUE) {}

  get events(): EventsProxy.EventsStream<EVENTS, NAME> {
    if (!this._events) this._events = {};
    const { _events } = this;

    return new Proxy(_events as EventsProxy.EventsStream<EVENTS, NAME>, {
      get: (target, p: string, receiver) => {
        if (p in target) return Reflect.get(target, p, receiver);
        const stream = new Stream({
          name: this.name + p[0].toUpperCase() + p.slice(1),
          consumerLeft(self) {
            if (self.consumersCount === 0) delete (_events as any)[p];
          },
        });
        (_events as any)[p] = stream;
        return stream;
      },
    });
  }
}

export namespace EventsProxy {
  export type EventsStream<EVENTS extends Record<string, unknown>, NAME extends string> = {
    [K in keyof EVENTS]: Stream<EVENTS[K], `${NAME}${Capitalize<K extends string ? K : "">}`>;
  };
  export type Hooks<EVENTS extends Record<string, unknown>, SELF> = {
    self: SELF;
    handlers: { [K in keyof EVENTS]: (self: SELF, value: EVENTS[K]) => void };
  };
}
