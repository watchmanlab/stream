import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { count } from "./count";
import { last } from "./last";
import { listen } from "./listen";

describe("count", () => {
  it("emits running count after each value", () => {
    const results: number[] = [];
    of("a", "b", "c")
      .pipe(count())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("emits final count via last()", () => {
    let result: any;
    of(1, 2, 3, 4, 5)
      .pipe(count())
      .pipe(last())
      .pipe(listen((v) => (result = v)));
    expect(result).toBe(5);
  });

  it("counts a single value", () => {
    const results: number[] = [];
    of("x")
      .pipe(count())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1]);
  });
});
