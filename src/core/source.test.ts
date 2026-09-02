import { describe, it, expect } from "bun:test";
import { Source } from "./source";
import { Stream } from "./stream";

describe("Source", () => {
  describe("pipe", () => {
    it("applies a transformer and returns its output", () => {
      const stream = new Stream<number>();
      const results: number[] = [];
      stream.pipe((input) => {
        input
          .consume((self, v) => {
            results.push(v * 2);
            self.next();
          })
          .next();
        return input;
      });
      stream.push(1).push(2).push(3);
      expect(results).toEqual([2, 4, 6]);
    });
  });

  describe("Symbol.asyncIterator", () => {
    it("yields values via for-await-of", async () => {
      const stream = new Stream<number>();
      const results: number[] = [];
      const iter = (async () => {
        for await (const v of stream) {
          results.push(v);
        }
      })();
      stream.push(1);
      stream.push(2);
      stream.push(3);
      stream.terminate("complete");
      await iter;
      expect(results).toEqual([1, 2, 3]);
    });
  });

  describe("Source.from", () => {
    it("wraps a consumable and exposes pipe", () => {
      const stream = new Stream<number>();
      const source = Source.from(stream);
      expect(typeof source.pipe).toBe("function");
      expect(typeof source.consume).toBe("function");
    });

    it("delegates consume to the wrapped consumable", () => {
      const stream = new Stream<number>();
      const source = Source.from(stream);
      const results: number[] = [];
      source
        .consume((self, v) => {
          results.push(v);
          self.next();
        })
        .next();
      stream.push(10).push(20);
      expect(results).toEqual([10, 20]);
    });
  });
});
