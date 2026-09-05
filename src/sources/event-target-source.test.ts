import { describe, it, expect, mock } from "bun:test";
import { fromEventTarget } from "./event-target-source";
import { listen } from "../transformers/listen";
import { Stream } from "../core/stream";

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

  it("should not emit when source is terminated", () => {
    const eventTarget = new EventTarget();
    let eventType = "";

    const fn = mock((e: Event) => (eventType = e.type));

    const stream = Stream.from(fromEventTarget(eventTarget, "click"));
    stream.pipe(listen(fn));

    stream.terminate("abort");

    const event = new Event("click");
    eventTarget.dispatchEvent(event);

    expect(fn).not.toHaveBeenCalled();
    expect(eventType).toBe("");
  });
});
