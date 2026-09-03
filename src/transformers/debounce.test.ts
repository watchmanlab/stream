import { describe, it, expect } from "bun:test";
import { debounce } from "./debounce";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("debounce", () => {
  it("emits only after silence window expires", async () => {
    const source = new Stream<number>();
    const results: number[] = [];
    source.pipe(debounce(50)).pipe(listen((v) => results.push(v)));
    source.push(1);
    source.push(2);
    source.push(3);
    await new Promise((r) => setTimeout(r, 100));
    expect(results).toEqual([3]);
  });

  it("resets timer on each new value", async () => {
    const source = new Stream<number>();
    const results: number[] = [];
    source.pipe(debounce(50)).pipe(listen((v) => results.push(v)));
    source.push(1);
    await new Promise((r) => setTimeout(r, 30));
    source.push(2);
    await new Promise((r) => setTimeout(r, 30));
    source.push(3);
    await new Promise((r) => setTimeout(r, 100));
    expect(results).toEqual([3]);
  });

  it("emits each value if spaced further apart than window", async () => {
    const source = new Stream<number>();
    const results: number[] = [];
    source.pipe(debounce(30)).pipe(listen((v) => results.push(v)));
    source.push(1);
    await new Promise((r) => setTimeout(r, 60));
    source.push(2);
    await new Promise((r) => setTimeout(r, 60));
    expect(results).toEqual([1, 2]);
  });
});
