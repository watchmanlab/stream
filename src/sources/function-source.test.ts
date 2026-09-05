import { describe, it, expect, mock } from "bun:test";
import { fromFunction } from "./function-source";

describe("fromFunction source", () => {
  it("should emit a two random values", () => {
    const fn = mock((v: string) => {});
    const source = fromFunction(() => Math.random().toFixed(2));
    source
      .consume((c, v) => {
        fn(v);
        c.next();
      })
      .next();
    source
      .consume((c, v) => {
        fn(v);
        c.next();
      })
      .next();

    expect(fn).toBeCalledTimes(2);
  });
  it("should terminate with complete after each execution ", () => {
    const fn = mock((v: string) => {});
    const source = fromFunction(() => Math.random().toFixed(2));
    const c1 = source
      .consume((c, v) => {
        fn(v);
        c.next();
      })
      .next();
    const c2 = source
      .consume((c, v) => {
        fn(v);
        c.next();
      })
      .next();

    expect(c1.status).toBe("complete");
    expect(c2.status).toBe("complete");
  });
});
