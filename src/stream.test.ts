import { it, expect, describe } from "bun:test";
import { Controller, Stream } from "./stream";
import { abortSignal } from "./transformers/abort-signal";
import { weakRef } from "../weak-ref";

describe("Stream", () => {
  describe("Constructor", () => {
    it("creates empty stream", () => {
      const stream = new Stream<number>();
      expect(stream.consumersCount).toBe(0);
    });

    it("creates stream with source function", () => {
      Stream;
      const stream = new Stream<number>(async function* () {
        yield 1;
        yield 2;
      });
      expect(stream.consumersCount).toBe(0);
    });

    it("should work with transformed streams in constructor", async () => {
      const source = new Stream<number>();
      const filtered = new Stream<number>(async function* () {
        for await (const value of source) {
          if (value > 0) yield value;
        }
      });

      const results: number[] = [];
      filtered.listen((value) => results.push(value));

      source.push(-1);
      source.push(1);
      source.push(-2);
      source.push(2);
      source.push(3);

      await new Promise((r) => setTimeout(r, 0));
      expect(results).toEqual([1, 2, 3]);
    });
  });

  describe("Push and Listen", () => {
    it("pushes and receives values", async () => {
      const stream = new Stream<number>();
      const values: number[] = [];

      stream.listen((value) => values.push(value));

      await stream.push(1, 2, 3);

      expect(values).toEqual([1, 2, 3]);
    });

    it("handles empty push", async () => {
      const stream = new Stream<number>();
      const values: number[] = [];

      stream.listen((value) => values.push(value));

      await stream.push(undefined as never);

      expect(values).toEqual([undefined as never]);
    });

    it("multiple listeners receive same values", async () => {
      const stream = new Stream<number>();
      const values1: number[] = [];
      const values2: number[] = [];

      stream.listen((value) => values1.push(value));
      stream.listen((value) => values2.push(value));

      await stream.push(1, 2);

      expect(values1).toEqual([1, 2]);
      expect(values2).toEqual([1, 2]);
    });

    it.only("consumer count property", async () => {
      const stream = new Stream<number>();

      expect(stream.consumersCount).toBe(0);

      const ctr = stream.listen(() => {});

      expect(stream.consumersCount).toBe(1);

      await ctr.abort();

      console.log(stream.consumersCount);

      expect(stream.consumersCount).toBe(0);
    });
  });

  describe("Cleanup Mechanisms", () => {
    it("manual cleanup removes listeners", async () => {
      const stream = new Stream<number>();
      const values: number[] = [];

      const controller = stream.listen((value) => values.push(value));

      stream.push(1);
      controller.abort();
      stream.push(2);

      await new Promise((r) => setTimeout(r, 0));
      expect(values).toEqual([1]);
      expect(stream.consumersCount).toBe(0);
    });

    it("respects aborted signal", async () => {
      const stream = new Stream<number>();
      const controller = new AbortController();
      controller.abort();

      const values: number[] = [];
      stream.listen((value) => values.push(value)).addSignal(new Stream().pipe(abortSignal(controller.signal)));

      stream.push(1);

      expect(values).toEqual([]);
    });

    it("aborts listener with signal", async () => {
      const stream = new Stream<number>();
      const controller = new AbortController();
      const values: number[] = [];

      stream.listen((value) => values.push(value)).addSignal(new Stream().pipe(abortSignal(controller.signal)));

      stream.push(1);
      controller.abort();
      await new Promise((r) => setTimeout(r, 0));
      stream.push(2);

      await new Promise((r) => setTimeout(r, 0));

      expect(values).toEqual([1]);
    });

    it("stream trigger cleanup", async () => {
      const stream = new Stream<number>();
      const stopSignal = new Stream<void>();
      const values: number[] = [];

      stream.listen((value) => values.push(value)).addSignal(stopSignal);

      stream.push(1);
      await stopSignal.push();

      await stream.push(2);

      expect(values).toEqual([1]);
      expect(stream.consumersCount).toBe(0);
    });
  });

  describe("WeakRef Context Support", () => {
    it("should auto-remove listener when context is garbage collected", async () => {
      const stream = new Stream<number>();
      const values: number[] = [];

      (() => {
        const context = { id: 1 };
        stream.listen((value) => values.push(value)).addSignal(new Stream().pipe(weakRef(context)));
      })();

      Bun.gc(true);
      await new Promise((resolve) => setTimeout(resolve, 10));

      await stream.push(1);

      expect(values).toEqual([]);
    });

    it("should handle multiple listeners with different contexts", async () => {
      const stream = new Stream<number>();
      const values1: number[] = [];
      const values2: number[] = [];

      (() => {
        const context1 = { id: 1 };
        const context2 = { id: 2 };

        stream.listen((value) => values1.push(value)).addSignal(new Stream().pipe(weakRef(context1)));
        stream.listen((value) => values2.push(value)).addSignal(new Stream().pipe(weakRef(context2)));
      })();

      Bun.gc(true);
      await new Promise((resolve) => setTimeout(resolve, 10));

      await stream.push(1, 2, 3);

      expect(values1).toEqual([]);
      expect(values2).toEqual([]);
    });

    it("should work with mixed signals types", async () => {
      const stream = new Stream<number>();
      const controller = new AbortController();
      const values1: number[] = [];
      const values2: number[] = [];
      const values3: number[] = [];

      (() => {
        const context = { id: 1 };
        stream.listen((value) => values1.push(value));
        stream.listen((value) => values2.push(value)).addSignal(new Stream().pipe(weakRef(context)));
        stream.listen((value) => values3.push(value)).addSignal(new Stream().pipe(abortSignal(controller.signal)));
      })();

      await stream.push(1, 2);

      expect(values1).toEqual([1, 2]);
      expect(values2).toEqual([1, 2]);
      expect(values3).toEqual([1, 2]);

      controller.abort();

      Bun.gc(true);
      await new Promise((resolve) => setTimeout(resolve, 20));

      await stream.push(3);

      expect(values1).toEqual([1, 2, 3]);
      expect(values2).toEqual([1, 2]);
      expect(values3).toEqual([1, 2]); // Aborted
    });
  });

  describe("generator Method", () => {
    it("should iterate while context is alive", async () => {
      const stream = new Stream<number>();
      const values: number[] = [];

      (async () => {
        const context = { id: 1 };
        for await (const value of stream.generator(new Controller().addSignal(new Stream().pipe(weakRef(context))))) {
          values.push(value);
        }
      })();

      await stream.push(1, 2, 3);

      Bun.gc(true);
      await new Promise((resolve) => setTimeout(resolve, 10));

      await stream.push(4);

      expect(values).toEqual([1, 2, 3]);
    });
  });

  describe("Next Interface", () => {
    it("next resolves with first value", async () => {
      const stream = new Stream<number>();

      const promise = stream.next();
      stream.push(5);

      const result = await promise;
      expect(result).toBe(5);
    });

    it("next is shared", async () => {
      const stream = new Stream<number>();

      const promise1 = stream.next();
      const promise2 = stream.next();

      stream.push(1);
      stream.push(2);

      const result = await promise1;
      const result2 = await promise2;
      expect(result).toBe(1);
      expect(result2).toBe(1);
    });
  });

  describe("Async Iteration", () => {
    it("supports for-await-of", async () => {
      const stream = new Stream<number>();
      const values: number[] = [];

      (async () => {
        let count = 0;
        for await (const value of stream) {
          values.push(value);
          if (++count === 3) break;
        }
      })();

      await stream.push(1, 2, 3, 4);

      expect(values).toEqual([1, 2, 3]);
    });
  });

  describe("Pipe Method ", () => {
    it("should allow transformers to return any type", async () => {
      const source = new Stream<number>();

      const stringResult = source.pipe(
        (stream) =>
          new Stream<string>(async function* () {
            for await (const value of stream) {
              yield value.toString();
            }
          }),
      );

      const results: string[] = [];
      stringResult.listen((value) => results.push(value));

      await source.push(1, 2, 3);

      expect(results).toEqual(["1", "2", "3"]);
    });
  });
});
