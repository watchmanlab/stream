import { describe, it, expect } from "bun:test";
import { fromGenerator } from "./generator-source";
import { listen } from "../transformers/listen";

describe("fromGenerator", () => {
  it("emits values yielded by the generator", () => {
    const results: number[] = [];
    fromGenerator(function* () {
      yield 1;
      yield 2;
      yield 3;
    }).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("creates a new generator per consumer", () => {
    const r1: number[] = [];
    const r2: number[] = [];
    const source = fromGenerator(function* () {
      yield 1;
      yield 2;
    });
    source.pipe(listen((v) => r1.push(v)));
    source.pipe(listen((v) => r2.push(v)));
    expect(r1).toEqual([1, 2]);
    expect(r2).toEqual([1, 2]);
  });

  it("completes when generator is done", () => {
    let status: string | undefined;
    fromGenerator(function* () {
      yield 1;
    })
      .consume((self, v) => self.next(), {
        terminate: (_, r) => (status = r),
      })
      .next();
    expect(status).toBe("complete");
  });

  it("emits nothing for empty generator", () => {
    const results: number[] = [];
    fromGenerator(function* () {}).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });
});
