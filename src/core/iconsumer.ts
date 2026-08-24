import type { TerminateReason } from "./types";

export interface Consumer<VALUE> extends Disposable {
  push(value: VALUE): this;
  next(): this;
  terminate(reason: TerminateReason): this;
}

export namespace Consumer {
  export type AnyConsumer = Consumer<any>;
}
