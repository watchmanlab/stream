import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { min } from "./min";
import { last } from "./last";
import { listen } from "./listen";

describe("min", () => {
  it("emits running minimum after each value", () => {
    const results: number[] = [];
    of(5, 3, 4, 1, 2)
      .pipe(min())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([5, 3, 3, 1, 1]);
  });

  it("emits final min via last()", () => {
    let result: any;
    of(5, 3, 8, 1, 9)
      .pipe(min())
      .pipe(last())
      .pipe(listen((v) => (result = v)));
    expect(result).toBe(1);
  });

  it("handles single value", () => {
    const results: number[] = [];
    of(7)
      .pipe(min())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([7]);
  });
});
