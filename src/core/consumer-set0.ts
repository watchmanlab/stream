import { Consumer } from "./consumer0";
import { TerminateReason } from "./types0";

export interface ConsumerSet<VALUE> {
  readonly size: number;
  push(value: VALUE): void;
  add(consumer: Consumer<VALUE>): ConsumerSet.Delete;
  terminate(reason: TerminateReason): void;
}
export namespace ConsumerSet {
  export type Delete = () => boolean;
}
