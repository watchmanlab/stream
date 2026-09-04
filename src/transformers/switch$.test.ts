import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { switch$ } from "./switch$";
import { listen } from "./listen";
import { Stream } from "../core/stream";
import { Consumable } from "../core/consumable";

describe("switch$", () => {
  it("does not work with a replayable sources", () => {
    const results: number[] = [];
    of(of(1, 2), of(3, 4))
      .pipe(switch$())
      .pipe(listen((v) => results.push(v)));

    expect(results).toEqual([]);
  });

  it("passes through non-consumable values directly", () => {
    const inner1 = of(1, 2);
    const inner2 = of("a", "b");
    const results: any[] = [];
    const stream = new Stream<number | Consumable<number> | Consumable<string>>();

    stream.pipe(switch$()).pipe(listen((v) => results.push(v)));

    stream.push(inner1);
    stream.push(3);
    stream.push(inner2);

    expect(results).toEqual([1, 2, 3, "a", "b"]);
  });

  it("cancels previous inner stream when new one arrives", () => {
    const inner1 = new Stream<number>();
    const inner2 = new Stream<number>();
    const outer = new Stream<any>();
    const results: number[] = [];

    outer.pipe(switch$()).pipe(listen((v) => results.push(v)));

    outer.push(inner1);
    inner1.push(1);
    outer.push(inner2); // cancels inner1
    inner1.push(99); // should be ignored
    inner2.push(2);

    expect(results).toEqual([1, 2]);
  });
});
