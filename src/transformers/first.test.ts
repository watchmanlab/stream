import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { first } from "./first";
import { listen } from "./listen";
import { Stream } from "../core/stream";
import { EMPTY } from "../core/consts";

describe("first", () => {
  it("emits the first value then terminates", () => {
    const results: any[] = [];
    of(1, 2, 3)
      .pipe(first())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1]);
  });

  it("emits value when predicate is matched then terminates", () => {
    const results: any[] = [];
    of(1, 2, 3)
      .pipe(first((v) => v > 1))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([2]);
  });

  it("emits EMPTY when predicate is not matched then terminates", () => {
    const results: any[] = [];
    of(1, 2, 3)
      .pipe(first((v) => v > 3))
      .pipe(listen((v) => results.push(v)));

    expect(results).toEqual([EMPTY]);
  });

  it("emits EMPTY if stream completes without values", () => {
    let result: any;
    const stream = new Stream<number>();
    stream.pipe(first()).pipe(listen((v) => (result = v)));
    stream.terminate("complete");
    expect(result).toEqual(EMPTY);
  });

  it("only emits once even with multiple values", () => {
    const results: any[] = [];
    of(10, 20, 30)
      .pipe(first())
      .pipe(listen((v) => results.push(v)));
    expect(results.length).toBe(1);
    expect(results[0]).toEqual(10);
  });
});
