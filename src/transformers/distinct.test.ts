import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { distinct } from "./distinct";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("distinct", () => {
  it("filters out consecutive and non-consecutive duplicates", () => {
    const results: number[] = [];
    of(1, 1, 2, 2, 3, 2, 1)
      .pipe(distinct())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("uses keySelector to compare by derived key", () => {
    const results: any[] = [];
    of({ id: 1, name: "a" }, { id: 1, name: "b" }, { id: 2, name: "c" })
      .pipe(distinct((v) => v.id))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([
      { id: 1, name: "a" },
      { id: 2, name: "c" },
    ]);
  });

  it("resets seen-set when $flushes notifier emits", () => {
    const results: number[] = [];
    const flusher = new Stream<void>();
    const source = new Stream<number>();

    source.pipe(distinct(undefined, flusher)).pipe(listen((v) => results.push(v)));

    source.push(1);
    source.push(1); // duplicate, skipped
    flusher.push(); // reset
    source.push(1); // now passes again

    expect(results).toEqual([1, 1]);
  });

  it("passes all values when all are unique", () => {
    const results: number[] = [];
    of(1, 2, 3, 4)
      .pipe(distinct())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3, 4]);
  });
});
