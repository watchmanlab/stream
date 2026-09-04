import { describe, it, expect } from "bun:test";
import { of } from "../sources/of-source";
import { safe } from "./safe";
import { map } from "./map";
import { filter } from "./filter";
import { listen } from "./listen";
import { Error } from "../core/types";

describe("safe", () => {
  it("passes values through when no error is thrown", () => {
    const results: any[] = [];
    of(1, 2, 3)
      .pipe(safe(map((v) => v * 2)))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([2, 4, 6]);
  });

  it("emits Error when handler throws", () => {
    const results: any[] = [];
    of(1, 2, 3)
      .pipe(
        safe(
          map((v) => {
            if (v === 2) throw "bad";
            return v;
          }),
        ),
      )
      .pipe(listen((v) => results.push(v)));
    expect(results[0]).toBe(1);
    expect(results[1]).toBeInstanceOf(Error);
    expect((results[1] as Error<any>).value).toBe("bad");
    expect(results[2]).toBe(3);
  });
  it("emits Error when error occur  a on deeper pipeline", () => {
    const results: any[] = [];
    of(1, 2, 3)
      .pipe(
        safe(($input) =>
          $input
            .pipe(filter((v) => v < 4))
            .pipe(
              map((v) => {
                if (v === 2) throw "bad";
                return v;
              }),
            )
            .pipe(filter((v) => v > 0)),
        ),
      )
      .pipe(listen((v) => results.push(v)));
    expect(results[0]).toBe(1);
    expect(results[1]).toBeInstanceOf(Error);
    expect((results[1] as Error<any>).value).toBe("bad");
    expect(results[2]).toBe(3);
  });

  it("do not crash when error thrown at the first element ", () => {
    const results: any[] = [];
    of(1)
      .pipe(
        safe(
          map((v) => {
            throw { code: 42 };
            return;
          }),
        ),
      )
      .pipe(listen((v) => results.push(v)));

    expect(results[0]).toBeInstanceOf(Error);
    expect((results[0] as Error<any>).value).toEqual({ code: 42 });
  });
});
