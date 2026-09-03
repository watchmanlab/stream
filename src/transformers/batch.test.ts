import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { batch } from "./batch";
import { listen } from "./listen";

describe("batch", () => {
  it("collects values into arrays of given size", () => {
    const results: number[][] = [];
    of(1, 2, 3, 4)
      .pipe(batch(2))
      .pipe(
        listen((v) => {
          results.push(v);
        }),
      );

    expect(results).toEqual([
      [1, 2],
      [3, 4],
    ]);
  });

  it("emits partial batch on completion", () => {
    const results: number[][] = [];
    of(1, 2, 3, 4, 5)
      .pipe(batch(2))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([[1, 2], [3, 4], [5]]);
  });

  it("emits single batch when size >= stream length", () => {
    const results: number[][] = [];
    of(1, 2, 3)
      .pipe(batch(10))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([[1, 2, 3]]);
  });

  it("emits one value per batch when size is 1", () => {
    const results: number[][] = [];
    of(1, 2, 3)
      .pipe(batch(1))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([[1], [2], [3]]);
  });
});
