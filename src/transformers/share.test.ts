import { describe, it, expect } from "bun:test";
import { share } from "./share";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("share", () => {
  it("converts a unicast source into a multicast stream", () => {
    const source = new Stream<number>();
    const shared = source.pipe(share());
    const r1: number[] = [];
    const r2: number[] = [];

    shared.pipe(listen((v) => r1.push(v)));
    shared.pipe(listen((v) => r2.push(v)));

    source.push(1);
    source.push(2);

    expect(r1).toEqual([1, 2]);
    expect(r2).toEqual([1, 2]);
  });

  it("returns a Stream instance", () => {
    const source = new Stream<number>();
    const shared = source.pipe(share());
    expect(shared).toBeInstanceOf(Stream);
  });

  it("late subscribers only receive values pushed after they subscribe", () => {
    const source = new Stream<number>();
    const shared = source.pipe(share());
    const r1: number[] = [];
    const r2: number[] = [];

    shared.pipe(listen((v) => r1.push(v)));
    source.push(1);
    shared.pipe(listen((v) => r2.push(v)));
    source.push(2);

    expect(r1).toEqual([1, 2]);
    expect(r2).toEqual([2]);
  });
});
