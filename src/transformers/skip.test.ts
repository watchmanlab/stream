import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { skip } from "./skip";
import { listen } from "./listen";

describe("skip", () => {
  it("skips the first N values", () => {
    const results: number[] = [];
    of(1, 2, 3, 4, 5)
      .pipe(skip(2))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([3, 4, 5]);
  });

  it("skip(0) passes all values", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(skip(0))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("skip more than available emits nothing", () => {
    const results: number[] = [];
    of(1, 2)
      .pipe(skip(10))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });
});
