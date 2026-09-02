import { describe, it, expect } from "bun:test";
import { DefaultConsumerSet } from "./default-consumer-set";
import { Consumer } from "./consumer";

function makeConsumer(results: number[]) {
  const c = new Consumer<number>((self, v) => {
    results.push(v);
    self.next();
  });
  c.next();
  return c;
}

describe("DefaultConsumerSet", () => {
  it("size is 0 initially", () => {
    const set = new DefaultConsumerSet<number>();
    expect(set.size).toBe(0);
  });

  it("push is no-op with 0 consumers", () => {
    const set = new DefaultConsumerSet<number>();
    expect(() => set.push(1)).not.toThrow();
  });

  it("adds a single consumer and pushes to it", () => {
    const results: number[] = [];
    const set = new DefaultConsumerSet<number>();
    set.add(makeConsumer(results));
    set.push(42);
    expect(results).toEqual([42]);
  });

  it("adds multiple consumers and broadcasts to all", () => {
    const r1: number[] = [];
    const r2: number[] = [];
    const set = new DefaultConsumerSet<number>();
    set.add(makeConsumer(r1));
    set.add(makeConsumer(r2));
    set.push(7);
    expect(r1).toEqual([7]);
    expect(r2).toEqual([7]);
  });

  it("delete removes a consumer", () => {
    const results: number[] = [];
    const set = new DefaultConsumerSet<number>();
    const c = makeConsumer(results);
    set.add(c);
    set.delete(c);
    set.push(1);
    expect(results).toEqual([]);
    expect(set.size).toBe(0);
  });

  it("delete returns false for unknown consumer", () => {
    const set = new DefaultConsumerSet<number>();
    const c = makeConsumer([]);
    expect(set.delete(c)).toBe(false);
  });

  it("terminate calls terminate on all consumers", () => {
    const set = new DefaultConsumerSet<number>();
    const r1: number[] = [];
    const r2: number[] = [];
    const c1 = makeConsumer(r1);
    const c2 = makeConsumer(r2);
    set.add(c1);
    set.add(c2);
    set.terminate("abort");
    expect(c1.status).toBe("abort");
    expect(c2.status).toBe("abort");
  });

  it("downgrades from Set to single consumer after delete", () => {
    const r1: number[] = [];
    const r2: number[] = [];
    const set = new DefaultConsumerSet<number>();
    const c1 = makeConsumer(r1);
    const c2 = makeConsumer(r2);
    set.add(c1);
    set.add(c2);
    set.delete(c1);
    expect(set.size).toBe(1);
    expect(set["_consumers"]).toBeInstanceOf(Consumer);
    set.push(5);
    expect(r2).toEqual([5]);
    expect(r1).toEqual([]);
  });
});
