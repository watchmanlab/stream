import { Stream } from "../../stream";

const NAME = "event-target";

type Name = typeof NAME;

export class EventTarget<EVENT_TYPE extends keyof HTMLElementEventMap | (string & {})> extends Stream<
  EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event,
  Name
> {
  constructor(target: globalThis.EventTarget, eventType: EVENT_TYPE) {
    let abortController: AbortController;

    super(NAME, async function* () {
      try {
        while (true) {
          yield await new Promise<any>((resolve) => {
            abortController = new AbortController();
            target.addEventListener(eventType, (e) => resolve(e), { once: true, signal: abortController.signal });
          });
        }
      } finally {
        abortController?.abort();
      }
    });

    // Cleanup hook for when consumers leave
    this._onEndConsuming = () => {
      abortController?.abort();
    };
  }
}

const button = new globalThis.EventTarget();

const e = new EventTarget(button, "click");

e.listen((v) => console.log(v.type)); //.push();

button.dispatchEvent(new Event("click"));
button.dispatchEvent(new Event("click"));
button.dispatchEvent(new Event("click"));
setTimeout(() => {
  button.dispatchEvent(new Event("click"));
});
