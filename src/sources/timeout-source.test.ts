import { describe, it, expect } from "bun:test";
import { fromTimeout } from "./timeout-source";
import { listen } from "../transformers/listen";

describe("fromTimeout", () => {
  it("emits a value after the specified delay", async () => {
    const results: string[] = [];
    await new Promise<void>((done) => {
      fromTimeout(30, "hello").pipe(
        listen((v) => {
          results.push(v);
          done();
        }),
      );
    });
    expect(results).toEqual(["hello"]);
  });

  it("emits undefined when no value is provided", async () => {
    let result: any = "not-set";
    await new Promise<void>((done) => {
      fromTimeout(20).pipe(
        listen((v) => {
          result = v;
          done();
        }),
      );
    });
    expect(result).toBeUndefined();
  });

  it("completes after emitting", async () => {
    let status: string | undefined;
    await new Promise<void>((done) => {
      fromTimeout(20, 1)
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

  it("emits only once", async () => {
    const results: number[] = [];
    await new Promise<void>((done) => {
      fromTimeout(20, 42).pipe(
        listen((v) => {
          results.push(v);
          done();
        }),
      );
    });
    await new Promise((r) => setTimeout(r, 40));
    expect(results.length).toBe(1);
  });
});
