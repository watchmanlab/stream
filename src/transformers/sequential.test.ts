import { describe, expect, test } from "bun:test";
import { Stream } from "../core/stream";
import { sequential } from "./sequential";

describe("sequential", () => {
  test("resolves promises sequentially", async () => {
    const stream = new Stream<Promise<number>>();
    const results: number[] = [];

    stream.pipe(sequential()).listen((self, value) => {
      results.push(value);
      self.next();
    });

    stream.push(Promise.resolve(1));
    stream.push(Promise.resolve(2));
    stream.push(Promise.resolve(3));

    await new Promise((r) => setTimeout(r, 100));
    expect(results).toEqual([1, 2, 3]);
  });

  test("waits for each promise before pulling next", async () => {
    const stream = new Stream<Promise<number>>();
    const order: string[] = [];

    stream.pipe(sequential()).listen((self, value) => {
      order.push(`resolved:${value}`);
      self.next();
    });

    const p1 = new Promise((r) => setTimeout(r, 100)).then(() => 1);
    const p2 = Promise.resolve(2);

    stream.push(p1);
    stream.push(p2);

    await new Promise((r) => setTimeout(r, 200));

    expect(order).toEqual([
      "resolved:1", // p1 resolves first

      "resolved:2",
    ]);
  });

  test("handles promise rejection", async () => {
    const stream = new Stream<Promise<number>>();
    const results: number[] = [];

    stream.pipe(sequential()).listen((self, value) => {
      results.push(value);
      self.next();
    });

    stream.push(Promise.resolve(1));
    stream.push(Promise.reject(2));
    stream.push(Promise.resolve(3));

    await new Promise((r) => setTimeout(r, 100));

    expect(results).toEqual([1, 3]);
  });
});
