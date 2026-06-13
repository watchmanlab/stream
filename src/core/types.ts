import type { Consumer } from "./consumer";
import type { Smoker } from "./smoker";

export interface Queue<VALUE> extends Iterable<VALUE>, Disposable {
  enqueue(value: VALUE): void;
  dequeue(): VALUE | Queue.Empty;
  values(): Queue.Iterator<VALUE>;
  clear(): void;
  readonly size: number;
}
export namespace Queue {
  export type Iterator<VALUE> = {
    next: () =>
      | {
          value: VALUE;
          done?: false;
        }
      | {
          value: Empty;
          done: true;
        };
  };
  export const EMPTY = Symbol.for("empty");
  export type Empty = typeof EMPTY;
}

export interface Source<VALUE> {
  readonly aborted: Smoker<"">;
  listen<ERROR>(
    handler: Consumer.Handler<VALUE, ERROR>,
    init?: Omit<Consumer.Init<VALUE, ERROR>, "handler">,
  ): Consumer.Abort<ERROR>;
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Consumer.Abort<ERROR>;
}
