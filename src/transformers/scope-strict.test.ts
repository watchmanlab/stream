import { describe, it, expect } from "bun:test";
import { scopeStrict } from "./scope-strict";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("scopeStrict", () => {
  it("does not terminate until ALL notifiers complete", () => {
    const source = new Stream<number>();
    const n1 = new Stream<void>();
    const n2 = new Stream<void>();
    const results: number[] = [];

    source.pipe(scopeStrict(n1, n2)).pipe(listen((v) => results.push(v)));

    source.push(1);
    n1.terminate("complete"); // only one done
    source.push(2); // still passes
    n2.terminate("complete"); // now both done
    source.push(3); // should be blocked

    expect(results).toEqual([1, 2]);
  });

  it("terminates immediately if all notifiers complete at once", () => {
    const source = new Stream<number>();
    const n1 = new Stream<void>();
    const n2 = new Stream<void>();
    const results: number[] = [];

    source.pipe(scopeStrict(n1, n2)).pipe(listen((v) => results.push(v)));

    source.push(1);
    n1.terminate("complete");
    n2.terminate("complete");
    source.push(2);

    expect(results).toEqual([1]);
  });
});
