import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { takeWhile } from "./take-while";
import { listen } from "./listen";

describe("takeWhile", () => {
  it("takes values while predicate is true", () => {
    const results: number[] = [];
    of(1, 2, 3, 4, 5)
      .pipe(takeWhile((v) => v < 4))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("emits nothing if first value fails predicate", () => {
    const results: number[] = [];
    of(5, 1, 2)
      .pipe(takeWhile((v) => v < 3))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });

  it("emits all if predicate always true", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(takeWhile(() => true))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });
});
