import { Stream } from "./stream";
import { Named } from "./types";

export class EventsLinker<EVENTS extends Record<string, unknown>, NAME extends string, CONTEXT extends Named> {
  protected _events: Partial<EventsLinker.EventStreams<EVENTS, NAME>> = {};
  constructor(
    private context: CONTEXT,
    functions?: EventsLinker.EventsFunctions<EVENTS, CONTEXT>,
  ) {
    if (functions) {
      this.emit = (eventName, value) => {
        functions[eventName]?.(context, value);
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
  get events(): EventsLinker.EventStreams<EVENTS, NAME> {
    const { _events } = this;

    return new Proxy(_events as EventsLinker.EventStreams<EVENTS, NAME>, {
      get: (target, p: string, receiver) => {
        if (p in target) return Reflect.get(target, p, receiver);
        const stream = new Stream({
          name: this.context.name + p[0].toUpperCase() + p.slice(1),
          events: {
            consumerLeft(context) {
              if (context.consumersCount === 0) delete (_events as any)[p];
            },
          },
        });
        (_events as any)[p] = stream;
        return stream;
      },
    });
  }
  abort(error?: any) {
    for (const event of Object.values(this._events)) (event as Stream.AnyStream).abort(error);
  }
  complete() {
    for (const event of Object.values(this._events)) (event as Stream.AnyStream).complete();
  }
}

export namespace EventsLinker {
  export type EventStreams<EVENTS extends Record<string, unknown>, NAME extends string> = {
    [K in keyof EVENTS]: Stream<EVENTS[K], `${NAME}${Capitalize<K extends string ? K : "">}`>;
  };
  export type EventsFunctions<EVENTS extends Record<string, unknown>, CONTEXT> = {
    [K in keyof EVENTS]?: (context: CONTEXT, value: EVENTS[K]) => void;
  };
}
