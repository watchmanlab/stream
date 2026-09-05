import { describe, it, expect } from "bun:test";
import { fromAsyncIterable } from "./async-iterable-source";

describe("fromAsyncIterable", () => {
  it("emits values from an async iterable", async () => {
    const results: number[] = [];

    const iterable: AsyncIterable<number> = {
      [Symbol.asyncIterator]() {
        let i = 0;
        return {
          next: () => Promise.resolve(++i > 3 ? { value: undefined as any, done: true } : { value: i }),
        };
      },
    };

    await new Promise<void>((done) => {
      fromAsyncIterable(iterable)
        .consume(
          (self, v) => {
            results.push(v);
            self.next();
          },
          { terminate: () => done() },
        )
        .next();
    });

    expect(results).toEqual([1, 2, 3]);
  });

  it("creates a new iterator per consumer", async () => {
    const r1: number[] = [];
    const r2: number[] = [];

    const makeIterable = (): AsyncIterable<number> => ({
      [Symbol.asyncIterator]() {
        let i = 0;
        return {
          next: () => Promise.resolve(++i > 2 ? { value: undefined as any, done: true } : { value: i }),
        };
      },
    });

    const source = fromAsyncIterable(makeIterable());

    await new Promise<void>((done) => {
      let finished = 0;
      const check = () => {
        if (++finished === 2) done();
      };
      source
        .consume(
          (self, v) => {
            r1.push(v);
            self.next();
          },
          { terminate: check },
        )
        .next();
      source
        .consume(
          (self, v) => {
            r2.push(v);
            self.next();
          },
          { terminate: check },
        )
        .next();
    });

    expect(r1).toEqual([1, 2]);
    expect(r2).toEqual([1, 2]);
  });
});
