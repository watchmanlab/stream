import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { scan } from "./scan";
import { listen } from "./listen";
import { last } from "./last";

describe("scan", () => {
  it("emits running accumulator after each value", () => {
    const results: number[] = [];
    of(1, 2, 3, 4)
      .pipe(scan(0, (acc, v) => acc + v))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 3, 6, 10]);
  });

  it("uses initial accumulator correctly", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(scan(10, (acc, v) => acc + v))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([11, 13, 16]);
  });

  it("works with non-numeric accumulator", () => {
    const results: string[] = [];
    of("a", "b", "c")
      .pipe(scan("", (acc, v) => acc + v))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual(["a", "ab", "abc"]);
  });

  it("emits one value per input value", () => {
    const results: number[] = [];
    of(5)
      .pipe(scan(0, (acc, v) => acc + v))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([5]);
  });
});
