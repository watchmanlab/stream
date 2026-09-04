import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { fromGenerator } from "../sources/generator-source";
import { resolve } from "./resolve";
import { listen } from "./listen";
import { Error } from "../core/types";

describe("resolve", () => {
  it("resolves promises and emits values", async () => {
    const results: any[] = [];
    await new Promise<void>((done) => {
      of(Promise.resolve(1), Promise.resolve(2), Promise.resolve(3))
        .pipe(resolve(3))
        .pipe(
          listen((v) => {
            results.push(v);
            if (results.length === 3) done();
          }),
        );
    });

    expect(results).toEqual([1, 2, 3]);
  });

  it("emits Error for rejected promises", async () => {
    const results: any[] = [];
    await new Promise<void>((done) => {
      of(Promise.reject("fail"), Promise.resolve(2))
        .pipe(resolve(2))
        .pipe(
          listen((v) => {
            results.push(v);
            if (results.length === 2) done();
          }),
        );
    });

    expect(results[1]).toBeInstanceOf(Error);
    expect((results[1] as Error<any>).value).toBe("fail");
    expect(results[0]).toBe(2);
  });

  it("respects concurrency limit", async () => {
    let active = 0;

    const results: number[] = [];

    const makePromise = (v: number) => {
      return new Promise<number>((res) => {
        active++;
        setTimeout(() => {
          res(v);
        }, 10);
      });
    };

    await new Promise<void>((done) => {
      fromGenerator(function* () {
        yield makePromise(1);
        yield makePromise(2);
        yield makePromise(3);
        yield makePromise(4);
      })
        .pipe(resolve(2))
        .consume((c, v) => {
          results.push(v as number);
          if (results.length === 2) {
            c.terminate("complete");
            done();
          } else {
            c.next();
          }
        })
        .next();
    });
    // the reason why 3 and not 2 is because `resolve` prefetch
    //the next promise immediately when concurrency is below the limit (2 in this case).
    expect(active).toBe(3);
    expect(results.length).toBe(2);
  });
});
