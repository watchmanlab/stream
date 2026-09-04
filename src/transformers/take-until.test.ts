import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { takeUntil } from "./take-until";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("takeUntil", () => {
  it("takes values until notifier emits", () => {
    const notifier = new Stream<void>();
    const source = new Stream<number>();
    const results: number[] = [];

    source.pipe(takeUntil(notifier)).pipe(listen((v) => results.push(v)));

    source.push(1);
    source.push(2);
    notifier.push();
    source.push(3);

    expect(results).toEqual([1, 2]);
  });

  it("terminates when notifier completes", () => {
    const notifier = new Stream<void>();
    const source = new Stream<number>();
    const results: number[] = [];

    source.pipe(takeUntil(notifier)).pipe(listen((v) => results.push(v)));
    source.push(1);
    notifier.terminate("complete");
    source.push(2);

    expect(results).toEqual([1]);
  });

  it("emits all values if notifier never fires", () => {
    const notifier = new Stream<void>();
    const results: number[] = [];
    of(1, 2, 3)
      .pipe(takeUntil(notifier))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });
});
