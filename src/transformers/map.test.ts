import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { map } from "./map";
import { listen } from "./listen";

describe("map", () => {
  it("transforms each value", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(map((v) => v * 2))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([2, 4, 6]);
  });

  it("provides index as second argument", () => {
    const indices: number[] = [];
    of("a", "b", "c")
      .pipe(map((_, i) => i))
      .pipe(listen((v) => indices.push(v)));
    expect(indices).toEqual([0, 1, 2]);
  });

  it("supports type transformation", () => {
    const results: string[] = [];
    of(1, 2, 3)
      .pipe(map((v) => v.toFixed(2)))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual(["1.00", "2.00", "3.00"]);
  });

  it("passes through empty stream", () => {
    const results: number[] = [];
    of(1)
      .pipe(map((v) => v))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1]);
  });
});
