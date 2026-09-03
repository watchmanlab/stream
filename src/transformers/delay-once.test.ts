import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { delayOnce } from "./delay-once";
import { listen } from "./listen";

describe("delayOnce", () => {
  it("delays only the first value", async () => {
    const timestamps: number[] = [];
    const start = Date.now();
    await new Promise<void>((done) => {
      of(1, 2, 3)
        .pipe(delayOnce(80))
        .pipe(
          listen((v) => {
            timestamps.push(Date.now() - start);
            if (timestamps.length === 3) done();
          }),
        );
    });

    expect(timestamps[0]).toBeGreaterThanOrEqual(60);
    // subsequent values arrive immediately after the first
    expect(timestamps[2] - timestamps[0]).toBeLessThan(30);
  });

  it("emits all values in order", async () => {
    const results: number[] = [];
    await new Promise<void>((done) => {
      of(1, 2, 3)
        .pipe(delayOnce(30))
        .pipe(
          listen((v) => {
            results.push(v);
            if (results.length === 3) done();
          }),
        );
    });
    expect(results).toEqual([1, 2, 3]);
  });
});
