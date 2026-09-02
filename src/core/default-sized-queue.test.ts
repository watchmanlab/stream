import { describe, it, expect } from "bun:test";
import { DefaultSizedQueue } from "./default-sized-queue";

describe("DefaultSizedQueue", () => {
  it("enqueues up to maxSize without dropping", () => {
    const q = new DefaultSizedQueue<number>(3);
    q.enqueue(1);
    q.enqueue(2);
    q.enqueue(3);
    expect(q.size).toBe(3);
  });

  it("drops oldest by default when full", () => {
    const q = new DefaultSizedQueue<number>(3);
    q.enqueue(1);
    q.enqueue(2);
    q.enqueue(3);
    q.enqueue(4);
    expect(q.size).toBe(3);
    expect(q.dequeue()).toBe(2);
  });

  it("drops newest when dropStrategy is newest", () => {
    const q = new DefaultSizedQueue<number>(3, { dropStrategy: "newest" });
    q.enqueue(1);
    q.enqueue(2);
    q.enqueue(3);
    q.enqueue(4);
    expect(q.size).toBe(3);
    expect(q.dequeue()).toBe(1);
  });

  it("calls drop callback with dropped value (oldest strategy)", () => {
    const dropped: number[] = [];
    const q = new DefaultSizedQueue<number>(2, { drop: (_, v) => dropped.push(v) });
    q.enqueue(1);
    q.enqueue(2);
    q.enqueue(3);
    expect(dropped).toEqual([1]);
  });

  it("calls drop callback with dropped value (newest strategy)", () => {
    const dropped: number[] = [];
    const q = new DefaultSizedQueue<number>(2, {
      dropStrategy: "newest",
      drop: (_, v) => dropped.push(v),
    });
    q.enqueue(1);
    q.enqueue(2);
    q.enqueue(3);
    expect(dropped).toEqual([3]);
  });

  it("drops all values when maxSize is 0", () => {
    const dropped: number[] = [];
    const q = new DefaultSizedQueue<number>(0, { drop: (_, v) => dropped.push(v) });
    q.enqueue(1);
    q.enqueue(2);
    expect(q.size).toBe(0);
    expect(dropped).toEqual([1, 2]);
  });

  it("accepts initial values via options.values", () => {
    const q = new DefaultSizedQueue<number>(5, { values: [1, 2, 3] });
    expect(q.size).toBe(3);
  });
});
