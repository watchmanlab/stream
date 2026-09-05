import { describe, it, expect } from "bun:test";
import { fromInterval } from "./interval-source";
import { take } from "../transformers/take";
import { listen } from "../transformers/listen";

describe("fromInterval", () => {
  it("emits incrementing counter values", async () => {
    const results: number[] = [];
    await new Promise<void>((done) => {
      fromInterval(20)
        .pipe(take(3))
        .pipe(
          listen((v) => {
            results.push(v);
            if (results.length === 3) done();
          }),
        );
    });
    expect(results).toEqual([0, 1, 2]);
  });

  it("emits at roughly the correct interval", async () => {
    const timestamps: number[] = [];
    const start = Date.now();
    await new Promise<void>((done) => {
      fromInterval(50)
        .pipe(take(2))
        .pipe(
          listen((v) => {
            timestamps.push(Date.now() - start);
            if (timestamps.length === 2) done();
          }),
        );
    });
    expect(timestamps[1] - timestamps[0]).toBeGreaterThanOrEqual(40);
  });

  it("stops emitting after consumer terminates", async () => {
    const results: number[] = [];
    await new Promise<void>((done) => {
      fromInterval(20)
        .pipe(take(2))
        .pipe(
          listen((v) => {
            results.push(v);
            if (results.length === 2) done();
          }),
        );
    });
    await new Promise((r) => setTimeout(r, 60));
    expect(results.length).toBe(2);
  });
});
