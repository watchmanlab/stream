import { Stream } from "./stream";
import { Evented } from "./types";

export class EventsLinker<EVENTS extends Record<string, unknown>, NAME extends string, TARGET extends Evented<any>> {
  protected _events?: Partial<EventsLinker.EventsStream<EVENTS, NAME>>;
  constructor(
    private name: NAME,
    target?: TARGET,
    functions?: EventsLinker.EventsFunctions<EVENTS, TARGET>,
  ) {
    if (functions && target) {
      this.emit = (eventName, value) => {
        functions[eventName]?.(target, value);
        this._events?.[eventName]?.push?.(value);
      };
    } else {
      this.emit = (eventName, value) => {
        this._events?.[eventName]?.push?.(value);
      };
    }
  }

  emit<KEY extends keyof EVENTS, VALUE extends EVENTS[KEY]>(eventName: KEY, value: VALUE): void {}
  has<KEY extends keyof EVENTS>(eventName: KEY): boolean {
    return this._events?.[eventName] !== undefined;
  }
  get events(): EventsLinker.EventsStream<EVENTS, NAME> {
    if (!this._events) this._events = {};
    const { _events } = this;

    return new Proxy(_events as EventsLinker.EventsStream<EVENTS, NAME>, {
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

export namespace EventsLinker {
  export type AnyEventsLinker = EventsLinker<any, any, any>;
  export type EventsStream<EVENTS extends Record<string, unknown>, NAME extends string> = {
    [K in keyof EVENTS]: Stream<EVENTS[K], `${NAME}${Capitalize<K extends string ? K : "">}`>;
  };
  export type EventsFunctions<EVENTS extends Record<string, unknown>, TARGET> = {
    [K in keyof EVENTS]?: (self: TARGET, value: EVENTS[K]) => void;
  };
}
