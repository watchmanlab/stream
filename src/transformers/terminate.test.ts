import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { terminate } from "./terminate";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("terminate", () => {
  it("emits complete reason when stream completes", () => {
    let result: any;
    of(1, 2, 3)
      .pipe(terminate())
      .pipe(listen((v) => (result = v)));
    expect(result).toBe("complete");
  });

  it("emits abort reason when stream aborts", () => {
    let result: any;
    const source = new Stream<number>();
    source.pipe(terminate()).pipe(listen((v) => (result = v)));
    source.terminate("abort");
    expect(result).toBe("abort");
  });

  it("ignores all values — only emits on termination", () => {
    const results: any[] = [];
    of(1, 2, 3)
      .pipe(terminate())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual(["complete"]);
  });

  it("emits exactly once", () => {
    const results: any[] = [];
    of(1, 2, 3, 4, 5)
      .pipe(terminate())
      .pipe(listen((v) => results.push(v)));
    expect(results.length).toBe(1);
  });
});
