import { describe, it, expect } from "bun:test";
import { fromRange } from "./range-source";
import { listen } from "../transformers/listen";

describe("fromRange", () => {
  it("emits integers from start (inclusive) to end (exclusive)", () => {
    const results: number[] = [];
    fromRange(1, 5).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3, 4]);
  });

  it("emits nothing when start equals end", () => {
    const results: number[] = [];
    fromRange(3, 3).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });

  it("emits nothing when start > end", () => {
    const results: number[] = [];
    fromRange(5, 3).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });

  it("completes after emitting all values", () => {
    let status: string | undefined;
    fromRange(1, 3)
      .consume((self, v) => self.next(), {
        terminate: (_, r) => (status = r),
      })
      .next();
    expect(status).toBe("complete");
  });

  it("emits a single value when range is 1", () => {
    const results: number[] = [];
    fromRange(7, 8).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([7]);
  });
});
