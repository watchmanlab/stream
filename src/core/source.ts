import { Consumer } from "./consumer";
import { DefaultQueue } from "./default-queue";
import { AnySource, Consumable } from "./types";

export abstract class Source<VALUE> implements Consumable<VALUE>, AsyncIterable<VALUE> {
  async *[Symbol.asyncIterator]() {
    const buffer = new DefaultQueue<VALUE>();
    let resolve: (() => void) | undefined;

    const consumer = this.consume((_, value) => {
      buffer.enqueue(value);
      resolve?.();
    });

    try {
      while (consumer.status === "active" || consumer.status === "drain" || buffer.size > 0) {
        if (buffer.size === 0) {
          await new Promise<void>((r) => (resolve = r));
          resolve = undefined;
        }
        while (buffer.size > 0) {
          yield buffer.dequeue() as VALUE;
        }
        consumer.next();
      }
    } catch (e) {
      consumer.terminate("abort");
    } finally {
      consumer.terminate("complete");
    }
  }
  abstract consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
  pipe<OUTPUT extends AnySource>(transform: (input: this) => OUTPUT): OUTPUT {
    return transform(this);
  }
}
