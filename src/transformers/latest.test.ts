import { describe, it, expect } from "bun:test";
import { latest } from "./latest";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("latests", () => {
  it("delivers buffered values to a late consumer", () => {
    const source = new Stream<number>();
    const buffered = source.pipe(latest(2));

    source.push(1);
    source.push(2);
    source.push(3);

    const results: number[] = [];
    buffered.pipe(listen((v) => results.push(v)));

    expect(results).toEqual([2, 3]);
  });

  it("forwards live values after buffered ones", () => {
    const source = new Stream<number>();
    const buffered = source.pipe(latest(2));

    source.push(1);
    source.push(2);

    const results: number[] = [];
    buffered.pipe(listen((v) => results.push(v)));
    source.push(3);

    expect(results).toEqual([1, 2, 3]);
  });

  it("buffers at most N values", () => {
    const source = new Stream<number>();
    const buffered = source.pipe(latest(2));

    source.push(1);
    source.push(2);
    source.push(3);
    source.push(4);

    const results: number[] = [];
    buffered.pipe(listen((v) => results.push(v)));

    expect(results).toEqual([3, 4]);
  });

  it("delivers nothing if no values were pushed before subscribe", () => {
    const source = new Stream<number>();
    const buffered = source.pipe(latest(3));
    const results: number[] = [];
    buffered.pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });
});
