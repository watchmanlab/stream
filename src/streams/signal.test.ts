import { describe, it, expect } from "bun:test";
import { Signal } from "./signal";
import { listen } from "../transformers/listen";

describe("Signal", () => {
  it("emits the pushed value then terminates", () => {
    const signal = new Signal<string>();
    const results: string[] = [];
    signal
      .consume((c, v) => {
        results.push(v);
        c.next();
      })
      .next();

    signal.push("done");
    expect(results).toEqual(["done"]);
    expect(signal.status).toBe("complete");
  });

  it("terminates after the first push even with multiple consumers", () => {
    const signal = new Signal<number>();
    const r1: number[] = [];
    const r2: number[] = [];
    signal
      .consume((c, v) => {
        r1.push(v);
        c.next();
      })
      .next();
    signal
      .consume((c, v) => {
        r2.push(v);
        c.next();
      })
      .next();

    signal.push(42);
    expect(r1).toEqual([42]);
    expect(r2).toEqual([42]);
    expect(signal.status).toBe("complete");
  });

  it("ignores subsequent pushes after first", () => {
    const signal = new Signal<number>();
    const results: number[] = [];
    signal
      .consume((c, v) => {
        results.push(v);
        c.next();
      })
      .next();
    signal.push(1);
    signal.push(2); // no-op after termination
    expect(results).toEqual([1]);
  });
});
