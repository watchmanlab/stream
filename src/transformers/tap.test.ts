import { describe, it, expect, mock } from "bun:test";
import { of } from "../sources/of-source";
import { tap } from "./tap";
import { listen } from "./listen";

describe("tap", () => {
  it("calls callback for each value without modifying it", () => {
    const side: number[] = [];
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(tap((v) => side.push(v * 10)))
      .pipe(listen((v) => results.push(v)));
    expect(side).toEqual([10, 20, 30]);
    expect(results).toEqual([1, 2, 3]);
  });

  it("provides index as second argument", () => {
    const indices: number[] = [];
    of("a", "b", "c")
      .pipe(tap((_, i) => indices.push(i)))
      .pipe(listen());
    expect(indices).toEqual([0, 1, 2]);
  });

  it("does not affect stream termination", () => {
    const cb = mock(() => {});
    of(1).pipe(tap(cb)).pipe(listen());
    expect(cb).toHaveBeenCalledTimes(1);
  });
});
