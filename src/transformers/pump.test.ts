import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { pump } from "./pump";
import { tap } from "./tap";
import { map } from "./map";

describe("pump", () => {
  it("eagerly drains upstream and trigger the pipeline consumption", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(map((v) => v * 2))
      .pipe(tap((v) => results.push(v)))
      .pipe(pump());

    expect(results).toEqual([2, 4, 6]);
  });
});
