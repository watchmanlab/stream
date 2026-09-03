import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { filter } from "./filter";
import { listen } from "./listen";

describe("filter", () => {
  it("passes only matching values downstream", () => {
    const results: number[] = [];
    of(1, 2, 3, 4, 5)
      .pipe(filter((v) => v % 2 === 0))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([2, 4]);
  });

  it("$complements receives rejected values", () => {
    const complements: number[] = [];
    const f = of(1, 2, 3, 4, 5).pipe(filter((v) => v % 2 === 0));
    f.$complements.pipe(listen((v) => complements.push(v)));
    f.pipe(listen());
    expect(complements).toEqual([1, 3, 5]);
  });

  it("$complements is lazy — not allocated if unused", () => {
    const f = of(1, 2, 3).pipe(filter((v) => v > 1));
    f.pipe(listen());
    // @ts-ignore accessing private
    expect((f as any)._$complements).toBeUndefined();
  });

  it("inline complement callback receives rejected values", () => {
    const rejected: number[] = [];
    of(1, 2, 3, 4)
      .pipe(
        filter(
          (v) => v % 2 === 0,
          (v) => rejected.push(v),
        ),
      )
      .pipe(listen());
    expect(rejected).toEqual([1, 3]);
  });

  it("provides index to predicate", () => {
    const indices: number[] = [];
    of("a", "b", "c", "d")
      .pipe(
        filter((_, i) => {
          indices.push(i);
          return true;
        }),
      )
      .pipe(listen());
    expect(indices).toEqual([0, 1, 2, 3]);
  });

  it("passes all values when predicate always true", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(filter(() => true))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("passes no values when predicate always false", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(filter(() => false))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });
});
