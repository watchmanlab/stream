import { describe, it, expect } from "bun:test";
import { Stream } from "./stream";
import { Consumer } from "./consumer";

describe("Stream", () => {
  describe("push / consume", () => {
    it("broadcasts to a single consumer", () => {
      const stream = new Stream<number>();
      const results: number[] = [];
      stream
        .consume((self, v) => {
          results.push(v);
          self.next();
        })
        .next();
      stream.push(1).push(2).push(3);
      expect(results).toEqual([1, 2, 3]);
    });

    it("broadcasts to multiple consumers", () => {
      const stream = new Stream<number>();
      const r1: number[] = [];
      const r2: number[] = [];
      stream
        .consume((self, v) => {
          r1.push(v);
          self.next();
        })
        .next();
      stream
        .consume((self, v) => {
          r2.push(v);
          self.next();
        })
        .next();
      stream.push(42);
      expect(r1).toEqual([42]);
      expect(r2).toEqual([42]);
    });

    it("push is no-op after termination", () => {
      const stream = new Stream<number>();
      const results: number[] = [];
      stream
        .consume((self, v) => {
          results.push(v);
          self.next();
        })
        .next();
      stream.terminate("abort");
      stream.push(99);
      expect(results).toEqual([]);
    });
  });

  describe("terminate", () => {
    it("abort terminates immediately", () => {
      const stream = new Stream<number>();
      stream.terminate("abort");
      expect(stream.status).toBe("abort");
    });

    it("complete transitions to drain then complete", () => {
      const stream = new Stream<number>();
      const results: number[] = [];
      stream
        .consume((self, v) => {
          results.push(v);
          self.next();
        })
        .next();
      stream.terminate("complete");
      expect(stream.status).toBe("complete");
    });

    it("consume after termination returns already-terminated consumer", () => {
      const stream = new Stream<number>();
      stream.terminate("abort");
      const c = stream.consume(() => {});
      expect(c.status).toBe("abort");
    });
  });

  describe("lifecycle events", () => {
    it("$push emits every pushed value", () => {
      const stream = new Stream<number>();
      const pushed: number[] = [];
      stream.$push
        .consume((self, v) => {
          pushed.push(v);
          self.next();
        })
        .next();
      stream.consume((self, v) => self.next()).next();
      stream.push(1).push(2);
      expect(pushed).toEqual([1, 2]);
    });

    it.only("$next emits when the fastest consumer pull", () => {
      const stream = new Stream<number>();
      const pullers: Consumer<any>[] = [];
      stream.$next
        .consume((self, c) => {
          pullers.push(c);
          self.next();
        })
        .next();
      const c1 = stream.consume((self, v) => self.next()).next();
      const c2 = stream.consume((self, v) => self.next()).next();
      stream.push(1).push(2);
      expect(pullers.length).toEqual(3);
      expect(pullers).toEqual([c1, c1, c1]);
    });

    it("$consumerJoin emits when a consumer subscribes", () => {
      const stream = new Stream<number>();
      let joined = 0;
      stream.$consumerJoin
        .consume((self) => {
          joined++;
          self.next();
        })
        .next();
      stream.consume((self) => self.next()).next();
      stream.consume((self) => self.next()).next();
      expect(joined).toBe(2);
    });

    it("$firstConsumerJoin emits only on first consumer", () => {
      const stream = new Stream<number>();
      let count = 0;
      stream.$firstConsumerJoin
        .consume((self) => {
          count++;
          self.next();
        })
        .next();
      stream.consume((self) => self.next()).next();
      stream.consume((self) => self.next()).next();
      expect(count).toBe(1);
    });

    it("$consumerLeft emits when a consumer terminates", () => {
      const stream = new Stream<number>();
      let left = 0;
      stream.$consumerLeft
        .consume((self) => {
          left++;
          self.next();
        })
        .next();
      const c = stream.consume((self) => self.next()).next();
      c.terminate("abort");
      expect(left).toBe(1);
    });

    it("$lastConsumerLeft emits when last consumer leaves", () => {
      const stream = new Stream<number>();
      let fired = false;
      stream.$lastConsumerLeft
        .consume((self) => {
          fired = true;
          self.next();
        })
        .next();
      const c = stream.consume((self) => self.next()).next();
      c.terminate("abort");
      expect(fired).toBe(true);
    });

    it("$terminate emits the reason on termination", () => {
      const stream = new Stream<number>();
      let reason: string | undefined;
      stream.$terminate
        .consume((self, r) => {
          reason = r;
          self.next();
        })
        .next();
      stream.terminate("complete");
      expect(reason).toBe("complete");
    });

    it("$terminate replays reason to late subscribers", () => {
      const stream = new Stream<number>();
      stream.terminate("abort");
      let reason: string | undefined;
      stream.$terminate
        .consume((self, r) => {
          reason = r;
          self.next();
        })
        .next();

      expect(reason).toBe("abort");
    });
  });

  describe("Stream.from", () => {
    it("bridges a consumable into a multicast stream", () => {
      const source = new Stream<number>();
      const bridged = Stream.from(source);
      const r1: number[] = [];
      const r2: number[] = [];
      bridged
        .consume((self, v) => {
          r1.push(v);
          self.next();
        })
        .next();
      bridged
        .consume((self, v) => {
          r2.push(v);
          self.next();
        })
        .next();
      source.push(1).push(2);
      expect(r1).toEqual([1, 2]);
      expect(r2).toEqual([1, 2]);
    });
  });

  describe("consumersCount", () => {
    it("tracks the number of active consumers", () => {
      const stream = new Stream<number>();
      expect(stream.consumersCount).toBe(0);
      const c = stream.consume((self) => self.next()).next();
      expect(stream.consumersCount).toBe(1);
      c.terminate("abort");
      expect(stream.consumersCount).toBe(0);
    });
  });

  describe("Symbol.dispose/asyncDispose", () => {
    it("terminates with abort", () => {
      const stream = new Stream<number>();
      stream[Symbol.dispose]();
      expect(stream.status).toBe("abort");
    });
    it("terminates with complete", async () => {
      const stream = new Stream<number>();
      await stream[Symbol.asyncDispose]();
      expect(stream.status).toBe("complete");
    });
  });
});
