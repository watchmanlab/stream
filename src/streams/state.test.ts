import { describe, it, expect } from "bun:test";
import { State, state } from "./state";
import { listen } from "../transformers/listen";

describe("State", () => {
  it("holds the initial value", () => {
    const s = new State(0);
    expect(s.value).toBe(0);
  });

  it("setting value pushes to consumers", () => {
    const s = state(0);
    const results: number[] = [];
    s.pipe(listen((v) => results.push(v)));
    s.value = 1;
    s.value = 2;
    expect(results).toEqual([1, 2]);
  });

  it("reading value returns the latest set value", () => {
    const s = state("a");
    s.value = "b";
    s.value = "c";
    expect(s.value).toBe("c");
  });

  it("state() factory creates a State instance", () => {
    const s = state(42);
    expect(s).toBeInstanceOf(State);
    expect(s.value).toBe(42);
  });
});
