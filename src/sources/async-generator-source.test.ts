import { describe, it, expect } from "bun:test";
import { fromAsyncGenerator } from "./async-generator-source";

describe("fromAsyncGenerator", () => {
  it("emits values yielded by the async generator", async () => {
    const results: number[] = [];

    await new Promise<void>((done) => {
      fromAsyncGenerator(async function* () {
        yield 1;
        yield 2;
        yield 3;
      })
        .consume(
          (self, v) => {
            results.push(v);
            self.next();
          },
          { terminate: () => done() },
        )
        .next();
    });

    expect(results).toEqual([1, 2, 3]);
  });

  it("creates a new generator per consumer", async () => {
    const r1: number[] = [];
    const r2: number[] = [];

    const source = fromAsyncGenerator(async function* () {
      yield 10;
      yield 20;
    });

    await new Promise<void>((done) => {
      let finished = 0;
      const check = () => {
        if (++finished === 2) done();
      };
      source
        .consume(
          (self, v) => {
            r1.push(v);
            self.next();
          },
          { terminate: check },
        )
        .next();
      source
        .consume(
          (self, v) => {
            r2.push(v);
            self.next();
          },
          { terminate: check },
        )
        .next();
    });

    expect(r1).toEqual([10, 20]);
    expect(r2).toEqual([10, 20]);
  });

  it("completes when generator is done", async () => {
    let status: string | undefined;

    await new Promise<void>((done) => {
      fromAsyncGenerator(async function* () {
        yield 1;
      })
        .consume((self, v) => self.next(), {
          terminate: (_, r) => {
            status = r;
            done();
          },
        })
        .next();
    });

    expect(status).toBe("complete");
  });
});
