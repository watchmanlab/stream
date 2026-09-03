import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { buffer } from "./buffer";
import { listen } from "./listen";

describe("buffer", () => {
  it("collects values into fixed-size non-overlapping windows", () => {
    const results: number[][] = [];
    of(1, 2, 3, 4)
      .pipe(buffer(2))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([
      [1, 2],
      [3, 4],
    ]);
  });

  it("emits partial buffer on completion", () => {
    const results: number[][] = [];
    of(1, 2, 3, 4, 5)
      .pipe(buffer(2))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([[1, 2], [3, 4], [5]]);
  });

  it("creates overlapping windows with startBufferEvery < size", () => {
    const results: number[][] = [];
    of(1, 2, 3, 4)
      .pipe(buffer(3, 1))
      .pipe(listen((v) => results.push(v)));

    // console.log(results);

    expect(results[0]).toEqual([1, 2, 3]);
    expect(results[1]).toEqual([2, 3, 4]);
  });

  it("emits single buffer when size >= stream length", () => {
    const results: number[][] = [];
    of(1, 2)
      .pipe(buffer(5))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([[1, 2]]);
  });
});
