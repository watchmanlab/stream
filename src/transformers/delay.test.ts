import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { delay } from "./delay";
import { listen } from "./listen";

describe("delay", () => {
  it("delays each value by ms milliseconds", async () => {
    const results: number[] = [];
    const start = Date.now();
    await new Promise<void>((done) => {
      of(1, 2, 3)
        .pipe(delay(50))
        .pipe(
          listen((v) => {
            results.push(v);
            if (results.length === 3) done();
          }),
        );
    });

    expect(Date.now() - start).toBeGreaterThanOrEqual(140);
    expect(results).toEqual([1, 2, 3]);
  });
});
