import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { max } from "./max";
import { last } from "./last";
import { listen } from "./listen";

describe("max", () => {
  it("emits running maximum after each value", () => {
    const results: number[] = [];
    of(3, 1, 4, 1, 5)
      .pipe(max())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([3, 3, 4, 4, 5]);
  });

  it("emits final max via last()", () => {
    let result: any;
    of(3, 1, 4, 1, 5, 9, 2, 6)
      .pipe(max())
      .pipe(last())
      .pipe(listen((v) => (result = v)));
    expect(result).toBe(9);
  });

  it("handles single value", () => {
    const results: number[] = [];
    of(7)
      .pipe(max())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([7]);
  });
});
