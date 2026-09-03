import { describe, it, expect } from "bun:test";
import { gate } from "./gate";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("gate", () => {
  it("opens when control emits true", () => {
    const source = new Stream<number>();
    const control = new Stream<boolean>();
    const results: number[] = [];

    source.pipe(gate(control)).pipe(listen((v) => results.push(v)));

    control.push(true);
    source.push(1);
    source.push(2);

    expect(results).toEqual([1, 2]);
  });

  it("closes when control emits false", () => {
    const source = new Stream<number>();
    const control = new Stream<boolean>();
    const results: number[] = [];

    source.pipe(gate(control)).pipe(listen((v) => results.push(v)));

    control.push(true);
    source.push(1);
    control.push(false);
    source.push(2); // blocked
    source.push(3); // blocked

    expect(results).toEqual([1]);
  });

  it("can re-open after closing", () => {
    const source = new Stream<number>();
    const control = new Stream<boolean>();
    const results: number[] = [];

    source.pipe(gate(control)).pipe(listen((v) => results.push(v)));

    control.push(true);
    source.push(1);
    control.push(false);
    source.push(2); // blocked
    control.push(true);
    source.push(3);

    expect(results).toEqual([1, 3]);
  });
});
