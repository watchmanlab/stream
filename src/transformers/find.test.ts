import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { find } from "./find";
import { listen } from "./listen";
import { Stream } from "../core/stream";
import { EMPTY } from "../core/consts";

describe("find", () => {
  it("emits the first matching value", () => {
    let result: any;
    of(1, 2, 3, 4)
      .pipe(find((v) => v > 2))
      .pipe(listen((v) => (result = v)));
    expect(result).toEqual(3);
  });

  it("emits EMPTY if no match found", () => {
    let result: any;
    of(1, 2, 3)
      .pipe(find((v) => v > 10))
      .pipe(listen((v) => (result = v)));
    expect(result).toEqual(EMPTY);
  });

  it("terminates after first match", () => {
    const results: any[] = [];
    of(1, 2, 3, 4, 5)
      .pipe(find((v) => v > 1))
      .pipe(listen((v) => results.push(v)));
    expect(results.length).toBe(1);
    expect(results[0]).toEqual(2);
  });

  it("provides index to predicate", () => {
    const indices: number[] = [];
    of("a", "b", "c")
      .pipe(
        find((_, i) => {
          indices.push(i);
          return false;
        }),
      )
      .pipe(listen());
    expect(indices).toEqual([0, 1, 2]);
  });

  it("emits EMPTY on empty stream", () => {
    let result: any;
    const stream = new Stream<number>();
    stream.pipe(find((v) => v > 0)).pipe(listen((v) => (result = v)));
    stream.terminate("complete");
    expect(result).toEqual(EMPTY);
  });
});
