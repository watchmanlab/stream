import { describe, it, expect } from "bun:test";
import { zip } from "./zip";
import { listen } from "./listen";
import { Stream } from "../core/stream";
import { EMPTY } from "../core/consts";

describe("zip", () => {
  it("emits tuples when all inputs have a value", () => {
    const s1 = new Stream<number>();
    const s2 = new Stream<string>();
    const results: any[] = [];

    s1.pipe(zip(s2)).pipe(listen((v) => results.push(v)));

    s1.push(1);
    s2.push("a");
    s1.push(2);
    s2.push("b");

    expect(results).toEqual([
      [1, "a"],
      [2, "b"],
    ]);
  });

  it("does not emit until all inputs have a value", () => {
    const s1 = new Stream<number>();
    const s2 = new Stream<string>();
    const results: any[] = [];

    s1.pipe(zip(s2)).pipe(listen((v) => results.push(v)));

    s1.push(1);
    expect(results).toEqual([]);
    s2.push("a");
    expect(results).toEqual([[1, "a"]]);
  });

  it("emits remaining unmatched values on $rest", () => {
    const s1 = new Stream<number>();
    const s2 = new Stream<string>();
    const rest: any[] = [];

    const zipped = s1.pipe(zip(s2));
    zipped.$rest.pipe(listen((v) => rest.push(v)));
    zipped.pipe(listen());

    s1.push(1);
    s2.push("a");
    s1.push(2);
    s1.terminate("complete");

    expect(rest.length).toBe(1);
    expect(rest[0][0]).toBe(2);
    expect(rest[0][1]).toBe(EMPTY);
  });

  it("zips three streams", () => {
    const s1 = new Stream<number>();
    const s2 = new Stream<string>();
    const s3 = new Stream<boolean>();
    const results: any[] = [];

    s1.pipe(zip(s2, s3)).pipe(listen((v) => results.push(v)));

    s1.push(1);
    s2.push("a");
    s3.push(true);

    expect(results).toEqual([[1, "a", true]]);
  });
});
