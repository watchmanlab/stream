import { describe, it, expect } from "bun:test";
import { pace } from "./pace";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("pace", () => {
  it("rate-limits output to one value per ms interval", async () => {
    const source = new Stream<number>();
    const timestamps: number[] = [];
    const start = Date.now();

    source.pipe(pace(60)).pipe(listen(() => timestamps.push(Date.now() - start)));

    source.push(1);
    source.push(2);
    source.push(3);

    await new Promise((r) => setTimeout(r, 250));

    expect(timestamps.length).toBeGreaterThanOrEqual(2);
    // gap between consecutive emissions should be >= 50ms
    expect(timestamps[1] - timestamps[0]).toBeGreaterThanOrEqual(50);
  });

  it("emits first value immediately", async () => {
    const source = new Stream<number>();
    const timestamps: number[] = [];
    const start = Date.now();

    source.pipe(pace(100)).pipe(listen(() => timestamps.push(Date.now() - start)));
    source.push(1);

    await new Promise((r) => setTimeout(r, 20));
    expect(timestamps[0]).toBeLessThan(20);
  });
});
