import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { skipUntil } from "./skip-until";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("skipUntil", () => {
  it("skips values until notifier emits", async () => {
    const source = new Stream<number>();
    const notifier = new Stream<void>();
    const results: number[] = [];

    source.pipe(skipUntil(notifier)).pipe(listen((v) => results.push(v)));

    source.push(1);
    source.push(2);
    notifier.push();
    source.push(3);
    source.push(4);

    expect(results).toEqual([3, 4]);
  });

  it("passes all values if notifier fires before any value", () => {
    const source = new Stream<number>();
    const notifier = new Stream<void>();
    const results: number[] = [];

    source.pipe(skipUntil(notifier)).pipe(listen((v) => results.push(v)));
    notifier.push();
    source.push(1);
    source.push(2);

    expect(results).toEqual([1, 2]);
  });

  it("skips all values if notifier never fires", () => {
    const results: number[] = [];
    const notifier = new Stream<void>();
    of(1, 2, 3)
      .pipe(skipUntil(notifier))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });
});
