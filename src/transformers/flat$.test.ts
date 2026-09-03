import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { flat$ } from "./flat$";
import { listen } from "./listen";

describe("flat$", () => {
  it("flattens a stream of consumables sequentially", () => {
    const results: number[] = [];
    of(of(1, 2), of(3, 4))
      .pipe(flat$())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3, 4]);
  });

  it("passes through non-consumable values directly", () => {
    const results: any[] = [];
    of(1, of(2, 3), 4)
      .pipe(flat$())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3, 4]);
  });

  it("waits for each inner stream to complete before subscribing to next", () => {
    const order: string[] = [];
    const s1 = of("a", "b");
    const s2 = of("c", "d");
    of(s1, s2)
      .pipe(flat$())
      .pipe(listen((v) => order.push(v)));
    expect(order).toEqual(["a", "b", "c", "d"]);
  });
});
