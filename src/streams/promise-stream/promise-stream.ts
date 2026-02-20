import { Stream } from "../stream";

const NAME = "promise-stream";

export class PromiseStream<VALUE, NAME extends string = PromiseStream.Name> extends Stream<
  PromiseStream.PromiseResult<VALUE>,
  NAME
> {
  constructor(promise: Promise<VALUE>, name = NAME as NAME) {
    super(name, async function* () {
      try {
        yield { status: "fulfilled", value: await promise };
      } catch (reason) {
        yield { status: "rejected", reason };
      }
    });
  }
}

export namespace PromiseStream {
  export type Name = typeof NAME;
  export type PromiseResult<VALUE> = { status: "fulfilled"; value: VALUE } | { status: "rejected"; reason: unknown };
  export function isFulfilled<VALUE>(result: PromiseResult<VALUE>): result is { status: "fulfilled"; value: VALUE } {
    return result.status === "fulfilled";
  }
  export function isRejected<VALUE>(result: PromiseResult<VALUE>): result is { status: "rejected"; reason: unknown } {
    return result.status === "rejected";
  }
}
