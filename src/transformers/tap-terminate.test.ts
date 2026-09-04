import { describe, it, expect, mock } from "bun:test";
import { of } from "../sources/of-source";
import { tapTerminate } from "./tap-terminate";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("tapTerminate", () => {
  it("calls callback with complete reason on stream completion", () => {
    const reasons: string[] = [];
    of(1, 2, 3)
      .pipe(tapTerminate((r) => reasons.push(r)))
      .pipe(listen());
    expect(reasons).toEqual(["complete"]);
  });

  it("calls callback with abort reason on abort", () => {
    const reasons: string[] = [];
    const source = new Stream<number>();
    source.pipe(tapTerminate((r) => reasons.push(r))).pipe(listen());
    source.terminate("abort");
    expect(reasons).toEqual(["abort"]);
  });

  it("passes values through unchanged", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(tapTerminate(() => {}))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("callback is called exactly once", () => {
    const cb = mock(() => {});
    of(1, 2, 3).pipe(tapTerminate(cb)).pipe(listen());
    expect(cb).toHaveBeenCalledTimes(1);
  });
});
