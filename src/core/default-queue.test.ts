import { describe, it, expect, mock } from "bun:test";
import { DefaultQueue } from "./default-queue";
import { EMPTY } from "./consts";

describe("DefaultQueue", () => {
  it("enqueues and dequeues in FIFO order", () => {
    const q = new DefaultQueue<number>();
    q.enqueue(1);
    q.enqueue(2);
    q.enqueue(3);
    expect(q.dequeue()).toBe(1);
    expect(q.dequeue()).toBe(2);
    expect(q.dequeue()).toBe(3);
  });

  it("returns EMPTY when dequeuing from empty queue", () => {
    const q = new DefaultQueue<number>();
    expect(q.dequeue()).toBe(EMPTY);
  });

  it("tracks size correctly", () => {
    const q = new DefaultQueue<number>();
    expect(q.size).toBe(0);
    q.enqueue(1);
    expect(q.size).toBe(1);
    q.enqueue(2);
    expect(q.size).toBe(2);
    q.dequeue();
    expect(q.size).toBe(1);
  });

  it("clear resets the queue", () => {
    const q = new DefaultQueue<number>();
    q.enqueue(1);
    q.enqueue(2);
    q.clear();
    expect(q.size).toBe(0);
    expect(q.dequeue()).toBe(EMPTY);
  });

  it("iterates values in FIFO order", () => {
    const q = new DefaultQueue<number>();
    q.enqueue(10);
    q.enqueue(20);
    q.enqueue(30);
    const result = [...q];
    expect(result).toEqual([10, 20, 30]);
  });

  it("accepts initial values via constructor", () => {
    const q = new DefaultQueue<number>([1, 2, 3]);
    expect(q.size).toBe(3);
    expect(q.dequeue()).toBe(1);
  });

  it("Symbol.dispose clears the queue", () => {
    const q = new DefaultQueue<number>();
    q.enqueue(1);
    q[Symbol.dispose]();
    expect(q.size).toBe(0);
  });
});
