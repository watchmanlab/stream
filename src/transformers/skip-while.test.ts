import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { skipWhile } from "./skip-while";
import { listen } from "./listen";

describe("skipWhile", () => {
  it("skips values while predicate is true", () => {
    const results: number[] = [];
    of(1, 2, 3, 4, 5)
      .pipe(skipWhile((v) => v < 3))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([3, 4, 5]);
  });

  it("passes all values if predicate is immediately false", () => {
    const results: number[] = [];
    of(5, 1, 2)
      .pipe(skipWhile((v) => v < 3))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([5, 1, 2]);
  });

  it("skips all if predicate always true", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(skipWhile(() => true))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });

  it("stops checking predicate after first false", () => {
    let checks = 0;
    of(1, 2, 3, 4)
      .pipe(
        skipWhile((v) => {
          checks++;
          return v < 2;
        }),
      )
      .pipe(listen());
    expect(checks).toBe(2);
  });
});
