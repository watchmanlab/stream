import { Stream } from "./stream";
import { Evented, EventsFunctions, EventsStreams, Named } from "./types";

export class EventsLinker<
  EVENTS extends Record<string, unknown>,
  NAME extends string,
  TARGET extends Evented<any> & Named,
> {
  protected _events?: Partial<EventsStreams<EVENTS, NAME>>;
  constructor(
    private target: TARGET,
    functions?: EventsFunctions<EVENTS, TARGET>,
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
  get events(): EventsStreams<EVENTS, NAME> {
    if (!this._events) this._events = {};
    const { _events } = this;

    return new Proxy(_events as EventsStreams<EVENTS, NAME>, {
      get: (target, p: string, receiver) => {
        if (p in target) return Reflect.get(target, p, receiver);
        const stream = new Stream({
          name: this.target.name + p[0].toUpperCase() + p.slice(1),
          events: {
            consumerLeft(self) {
              if (self.consumersCount === 0) delete (_events as any)[p];
            },
          },
        });
        (_events as any)[p] = stream;
        return stream;
      },
    });
  }
}
