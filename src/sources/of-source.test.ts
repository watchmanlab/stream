import { describe, it, expect } from "bun:test";
import { of } from "./of-source";
import { listen } from "../transformers/listen";

describe("of", () => {
  it("emits all provided values in order", () => {
    const results: number[] = [];
    of(1, 2, 3).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("completes after emitting all values", () => {
    let status: string | undefined;
    of(1, 2, 3)
      .consume((self, v) => self.next(), {
        terminate: (_, r) => (status = r),
      })
      .next();
    expect(status).toBe("complete");
  });

  it("works with a single value", () => {
    const results: number[] = [];
    of(42).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([42]);
  });

  it("works with mixed types", () => {
    const results: any[] = [];
    of(1, "a", true).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, "a", true]);
  });
});
