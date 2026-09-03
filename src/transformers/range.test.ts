import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { range } from "./range";
import { listen } from "./listen";

describe("range", () => {
  it("passes values at indices [start, start+offset)", () => {
    const results: string[] = [];
    of("a", "b", "c", "d", "e")
      .pipe(range(2, 3))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual(["c", "d", "e"]);
  });

  it("starts from index 0", () => {
    const results: number[] = [];
    of(10, 20, 30, 40)
      .pipe(range(0, 2))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([10, 20]);
  });

  it("emits nothing if start is beyond stream length", () => {
    const results: number[] = [];
    of(1, 2)
      .pipe(range(5, 3))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });
});
