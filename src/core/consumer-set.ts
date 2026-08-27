import { Consumer } from "./consumer";
import { TerminateReason } from "./types";

export interface ConsumerSet<VALUE> {
  readonly size: number;
  push(value: VALUE): void;
  add(consumer: Consumer<VALUE>): void;
  delete(consumer: Consumer<VALUE>): boolean;
  terminate(reason: TerminateReason): void;
}
export namespace ConsumerSet {
  export type AnyConsumer = Consumer<any>;
}
