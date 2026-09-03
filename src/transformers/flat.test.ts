import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { flat } from "./flat";
import { listen } from "./listen";

describe("flat", () => {
  it("flattens one level of arrays by default", () => {
    const results: number[] = [];
    of([1, 2], [3, 4])
      .pipe(flat())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3, 4]);
  });

  it("flattens nested arrays with depth 1", () => {
    const results: number[] = [];
    of([[1, 2], [3]], [[4]])
      .pipe(flat(1))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3, 4]);
  });

  it("skips empty arrays", () => {
    const results: number[] = [];
    of([], [1, 2], [])
      .pipe(flat())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2]);
  });

  it("emits single-element arrays correctly", () => {
    const results: number[] = [];
    of([1], [2], [3])
      .pipe(flat())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });
});
