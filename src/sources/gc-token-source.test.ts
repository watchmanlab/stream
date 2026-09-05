import { describe, it, expect } from "bun:test";
import { fromGCToken } from "./gc-token-source";

describe("fromGCToken", () => {
  it("should emit after a token is garbage collected", async () => {
    let result = "";
    const { promise, resolve } = Promise.withResolvers();

    {
      const object = {} as any;

      fromGCToken(object)
        .consume(() => {
          result = "kechmahaja";
          resolve();
        })
        .next();
    }
    Bun.gc(true);

    await promise;

    expect(result).toBe("kechmahaja");
  });
});
