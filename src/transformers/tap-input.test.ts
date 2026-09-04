import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { tapInput } from "./tap-input";
import { filter } from "./filter";
import { listen } from "./listen";

describe("tapInput", () => {
  it("runs side-effect with the raw input before pipeline", () => {
    const seen: any[] = [];
    of(1, 2, 3)
      .pipe(tapInput((input) => seen.push(input)))
      .pipe(listen());
    expect(seen.length).toBe(1);
  });

  it("returns the input unchanged", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(tapInput(() => {}))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("can access $complements of a filter via tapInput", () => {
    const complements: number[] = [];
    of(1, 2, 3, 4, 5)
      .pipe(filter((v) => v % 2 === 0))
      .pipe(tapInput((input) => input.$complements.pipe(listen((v) => complements.push(v)))))
      .pipe(listen());
    expect(complements).toEqual([1, 3, 5]);
  });
});
