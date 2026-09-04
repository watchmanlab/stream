import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { scanArray } from "./scan-array";
import { last } from "./last";
import { listen } from "./listen";
import { Stream } from "../core/stream";
import { EMPTY } from "../core/consts";

describe("scanArray", () => {
  it("accumulates values into a growing array", () => {
    const snapshots: number[][] = [];
    of(1, 2, 3)
      .pipe(scanArray())
      .pipe(listen((v) => snapshots.push([...v])));
    expect(snapshots).toEqual([[1], [1, 2], [1, 2, 3]]);
  });

  it("emits the final array via last()", () => {
    let result: any;
    of(1, 2, 3)
      .pipe(scanArray())
      .pipe(last())
      .pipe(listen((v) => (result = v)));
    expect(result).toEqual([1, 2, 3]);
  });

  it("emits EMPTY via last() on empty stream", () => {
    let result: any;
    new Stream()
      .terminate("complete")
      .pipe(scanArray())
      .pipe(last())
      .pipe(
        listen((v) => {
          result = v;
        }),
      );

    expect(result).toEqual(EMPTY);
  });
});
