import { Consumer } from "./consumer";
import { TerminateReason } from "./types";

export interface ConsumerSet<VALUE> {
  readonly size: number;
  push(value: VALUE): void;
  add(consumer: Consumer<VALUE>): ConsumerSet.Delete;
  terminate(reason: TerminateReason): void;
}
export namespace ConsumerSet {
  export type AnyConsumer = Consumer<any>;
  export type Delete = () => boolean;
}
