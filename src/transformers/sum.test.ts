import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { sum } from "./sum";
import { last } from "./last";
import { listen } from "./listen";

describe("sum", () => {
  it("emits running sum after each value", () => {
    const results: number[] = [];
    of(1, 2, 3, 4)
      .pipe(sum())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 3, 6, 10]);
  });

  it("emits final sum via last()", () => {
    const results: any[] = [];
    of(1, 2, 3, 4, 5)
      .pipe(sum())
      .pipe(last())
      .pipe(listen((v) => results.push(v)));
    expect(results.length).toBe(1);
    expect(results[0]).toBe(15);
  });

  it("handles negative numbers", () => {
    const results: number[] = [];
    of(10, -3, -2)
      .pipe(sum())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([10, 7, 5]);
  });

  it("handles single value", () => {
    const results: number[] = [];
    of(42)
      .pipe(sum())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([42]);
  });
});
