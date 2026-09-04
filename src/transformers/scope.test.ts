import { describe, it, expect } from "bun:test";
import { scope } from "./scope";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("scope", () => {
  it("terminates when the first consumable abort", () => {
    const source = new Stream<number>();
    const consumable = new Stream<void>();
    const results: number[] = [];

    source.pipe(scope(consumable)).pipe(listen((v) => results.push(v)));

    source.push(1);
    source.push(2);
    consumable.terminate("abort");
    source.push(3);

    expect(results).toEqual([1, 2]);
  });

  it("terminates when the first consumable completes", () => {
    const source = new Stream<number>();
    const consumable = new Stream<void>();
    const results: number[] = [];

    source.pipe(scope(consumable)).pipe(listen((v) => results.push(v)));

    source.push(1);
    consumable.terminate("complete");
    source.push(2);

    expect(results).toEqual([1]);
  });

  it("terminates on the first of multiple consumables", () => {
    const source = new Stream<number>();
    const c1 = new Stream<void>();
    const c2 = new Stream<void>();
    const results: number[] = [];

    source.pipe(scope(c1, c2)).pipe(listen((v) => results.push(v)));

    source.push(1);
    c2.terminate("complete"); // n2 terminate first
    source.push(2);

    expect(results).toEqual([1]);
  });

  it("passes all values if consumables never fires", () => {
    const consumable = new Stream<void>();
    const results: number[] = [];
    const source = new Stream<number>();

    source.pipe(scope(consumable)).pipe(listen((v) => results.push(v)));
    source.push(1);
    source.push(2);

    expect(results).toEqual([1, 2]);
  });
});
