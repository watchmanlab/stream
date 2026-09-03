import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { context } from "./context";
import { tap } from "./tap";
import { listen } from "./listen";

describe("context", () => {
  it("wraps each value with the context object", () => {
    const results: any[] = [];
    of(1, 2, 3)
      .pipe(context({ label: "test" }))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([
      { value: 1, context: { label: "test" } },
      { value: 2, context: { label: "test" } },
      { value: 3, context: { label: "test" } },
    ]);
  });

  it("shares the same context reference across all values", () => {
    const refs: object[] = [];
    of(1, 2, 3)
      .pipe(context({ count: 0 }))
      .pipe(listen((v) => refs.push(v.context)));
    expect(refs[0]).toBe(refs[1]);
    expect(refs[1]).toBe(refs[2]);
  });

  it("mutations to context are visible in subsequent values", () => {
    const counts: number[] = [];
    of(1, 2, 3)
      .pipe(context({ count: 0 }))
      .pipe(tap((v) => v.context.count++))
      .pipe(listen((v) => counts.push(v.context.count)));
    expect(counts).toEqual([1, 2, 3]);
  });
});
