import { describe, it, expect, mock } from "bun:test";
import { fromAbortController } from "./abort-controller-source";
import { listen } from "../transformers/listen";
describe("fromAbortController", () => {
  it("emit when abort controller abort", () => {
    const controller = new AbortController();
    const fn = mock(() => {});
    fromAbortController(controller).pipe(listen(fn));
    controller.abort();
    expect(fn).toHaveBeenCalled();
  });

  it("emit immediately when abort signal is already abort", () => {
    const controller = new AbortController();
    const fn = mock(() => {});
    controller.abort();
    fromAbortController(controller).pipe(listen(fn));
    expect(fn).toHaveBeenCalled();
  });
});
