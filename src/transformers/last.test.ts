import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { last } from "./last";
import { listen } from "./listen";
import { Stream } from "../core/stream";
import { EMPTY } from "../core/consts";

describe("last", () => {
  it("emits the last value on completion", () => {
    let result: any;
    of(1, 2, 3)
      .pipe(last())
      .pipe(listen((v) => (result = v)));
    expect(result).toBe(3);
  });

  it("emits EMPTY if stream completes without values", () => {
    let result: any = "not-set";
    const stream = new Stream<number>();
    stream.pipe(last()).pipe(listen((v) => (result = v)));
    stream.terminate("complete");
    expect(result).toBe(EMPTY);
  });

  it("emits EMPTY on abort", () => {
    let result: any = "not-set";
    const stream = new Stream<number>();
    stream.pipe(last()).pipe(listen((v) => (result = v)));
    stream.push(1);
    stream.terminate("abort");
    expect(result).toBe(EMPTY);
  });

  it("only emits once", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(last())
      .pipe(listen((v) => results.push(v as number)));
    expect(results.length).toBe(1);
  });
});
