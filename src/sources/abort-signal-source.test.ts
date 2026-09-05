import { describe, it, expect, mock } from "bun:test";
import { fromAbortSignal } from "./abort-signal-source";
import { listen } from "../transformers/listen";
describe("fromAbortSignal", () => {
  it("emit when abort signal abort", () => {
    const controller = new AbortController();
    const fn = mock(() => {});
    fromAbortSignal(controller.signal).pipe(listen(fn));
    controller.abort();
    expect(fn).toHaveBeenCalled();
  });

  it("emit immediately when abort signal is already abort", () => {
    const controller = new AbortController();
    const fn = mock(() => {});
    controller.abort();
    fromAbortSignal(controller.signal).pipe(listen(fn));
    expect(fn).toHaveBeenCalled();
  });
});
