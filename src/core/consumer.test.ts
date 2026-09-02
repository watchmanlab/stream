import { describe, it, expect, mock } from "bun:test";
import { Consumer } from "./consumer";

function collect<T>(setup: (consumer: Consumer<T>) => void): T[] {
  const results: T[] = [];
  const consumer = new Consumer<T>((self, v) => {
    results.push(v);
    self.next();
  });
  consumer.next();
  setup(consumer);
  return results;
}

describe("Consumer", () => {
  describe("credit system", () => {
    it("does not process values without credit", () => {
      const results: number[] = [];
      const consumer = new Consumer<number>((self, v) => {
        results.push(v);
        self.next();
      });
      consumer.push(1);
      consumer.push(2);
      expect(results).toEqual([]);
    });

    it("processes value immediately when credit is available", () => {
      const results: number[] = [];
      const consumer = new Consumer<number>((self, v) => {
        results.push(v);
        self.next();
      });
      consumer.next();
      consumer.push(1);
      expect(results).toEqual([1]);
    });

    it("queues values and drains in order when credit is granted", () => {
      const results: number[] = [];
      const consumer = new Consumer<number>((self, v) => {
        results.push(v);
        self.next();
      });
      consumer.push(1);
      consumer.push(2);
      consumer.push(3);
      consumer.next();
      expect(results).toEqual([1, 2, 3]);
    });

    it("tracks credit count correctly", () => {
      const consumer = new Consumer<number>((self, v) => {});
      consumer.next();
      consumer.next();
      expect(consumer.credit).toBe(2);
      consumer.push(1);
      expect(consumer.credit).toBe(1);
    });
  });

  describe("status lifecycle", () => {
    it("starts as active", () => {
      const consumer = new Consumer<number>(() => {});
      expect(consumer.status).toBe("active");
    });

    it("abort terminates immediately and clears queue", () => {
      const results: number[] = [];
      const consumer = new Consumer<number>((self, v) => {
        results.push(v);
        self.next();
      });
      consumer.push(1);
      consumer.push(2);
      consumer.terminate("abort");
      expect(consumer.status).toBe("abort");
      expect(consumer.queue).toEqual(undefined);
    });

    it("complete drains queue before terminating", () => {
      const results: number[] = [];
      const consumer = new Consumer<number>((self, v) => {
        results.push(v);
      });
      consumer.push(1);
      consumer.push(2);
      consumer.terminate("complete");
      expect(consumer.status).toBe("drain");
      consumer.next();
      consumer.next();

      expect(consumer.status).toBe("complete");
      expect(results).toEqual([1, 2]);
    });

    it("push is no-op after termination", () => {
      const results: number[] = [];
      const consumer = new Consumer<number>((self, v) => {
        results.push(v);
        self.next();
      });
      consumer.next();
      consumer.terminate("abort");
      consumer.push(99);
      expect(results).toEqual([]);
    });

    it("next is no-op after termination", () => {
      const consumer = new Consumer<number>(() => {});
      consumer.terminate("abort");
      expect(() => consumer.next()).not.toThrow();
      expect(consumer.credit).toBe(0);
    });
  });

  describe("options", () => {
    it("calls init on construction and cleanup on termination", () => {
      const cleanup = mock((reason: string) => {});
      const consumer = new Consumer<number>((self) => self.next(), { init: () => cleanup });
      consumer.terminate("complete");
      expect(cleanup).toHaveBeenCalledWith("complete");
    });

    it("calls next option when credit is granted with empty queue", () => {
      const onNext = mock(() => {});
      const consumer = new Consumer<number>((self) => self.next(), { next: onNext });
      consumer.next();
      expect(onNext).toHaveBeenCalled();
    });

    it("calls terminate option on termination", () => {
      const onTerminate = mock(() => {});
      const consumer = new Consumer<number>((self) => self.next(), { terminate: onTerminate });
      consumer.terminate("complete");
      expect(onTerminate).toHaveBeenCalledWith(consumer, "complete");
    });

    it("calls drain option when complete is called with queued values", () => {
      const onDrain = mock(() => {});
      const consumer = new Consumer<number>((self, v) => self.next(), { drain: onDrain });
      consumer.push(1);
      consumer.terminate("complete");
      expect(onDrain).toHaveBeenCalled();
    });
  });

  describe("pushBatch / pushMany", () => {
    it("pushBatch delivers all values in order", () => {
      const results = collect<number>((c) => c.pushBatch([1, 2, 3]));
      expect(results).toEqual([1, 2, 3]);
    });

    it("pushMany delivers all values in order", () => {
      const results = collect<number>((c) => c.pushMany(4, 5, 6));
      expect(results).toEqual([4, 5, 6]);
    });
  });

  describe("Symbol.dispose/asyncDipose", () => {
    it("terminates with abort via using keyword pattern", () => {
      const consumer = new Consumer<number>(() => {});
      consumer[Symbol.dispose]();
      expect(consumer.status).toBe("abort");
    });
    it("terminates with complete via await using keyword pattern", async () => {
      const consumer = new Consumer<number>(() => {});
      await consumer[Symbol.asyncDispose]();
      expect(consumer.status).toBe("complete");
    });
  });
});
