import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { passive } from "./passive";
import { listen } from "./listen";
import { Stream } from "../core/stream";
import { share } from "./share";

describe("passive", () => {
  it("receives values requested by an active consumer without driving the source", () => {
    const source = new Stream<number>();
    const active: number[] = [];
    const passive_results: number[] = [];

    source
      .consume((self, v) => {
        active.push(v);
        self.next();
      })
      .next();
    source.pipe(passive()).pipe(listen((v) => passive_results.push(v)));

    source.push(1);
    source.push(2);
    source.push(3);

    expect(active).toEqual([1, 2, 3]);
    expect(passive_results).toEqual([1, 2, 3]);
  });

  it("does not pull values on its own — no values without an active driver", () => {
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(passive())
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });

  it("works with share() to observe a hot stream passively", () => {
    const source = new Stream<number>();
    const shared = source.pipe(share());
    const driven: number[] = [];
    const observed: number[] = [];

    shared
      .consume((self, v) => {
        driven.push(v);
        self.next();
      })
      .next();
    shared.pipe(passive()).pipe(listen((v) => observed.push(v)));

    source.push(10);
    source.push(20);

    expect(driven).toEqual([10, 20]);
    expect(observed).toEqual([10, 20]);
  });
});
