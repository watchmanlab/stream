import { Consumer } from "./consumer";
import { TerminateReason } from "./types";

/**
 * A set of {@link Consumer}s that a {@link Stream} broadcasts values to.
 * Implementations handle fan-out and lifecycle management.
 *
 * @template VALUE The type of values pushed to consumers.
 */
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
