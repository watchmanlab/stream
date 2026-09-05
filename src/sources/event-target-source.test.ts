import { describe, it, expect, mock } from "bun:test";
import { fromEventTarget } from "./event-target-source";
import { listen } from "../transformers/listen";

describe("fromAbortSignal", () => {
  it("emit when event is dispateched by the event target", () => {
    const eventTarget = new EventTarget();
    let eventType = "";

    const fn = mock((e: Event) => (eventType = e.type));

    fromEventTarget(eventTarget, "click").pipe(listen(fn));

    const event = new Event("click");
    eventTarget.dispatchEvent(event);

    expect(fn).toHaveBeenCalledWith(event);
    expect(eventType).toBe("click");
  });
});
