import { Consumer } from "./consumer";

import { AnyConsumable, AnySource, Consumable } from "./types";

export abstract class Source<VALUE> implements Consumable<VALUE>, AsyncIterable<VALUE> {
  async *[Symbol.asyncIterator]() {
    let resolve: (value: VALUE) => void;

    const consumer = this.consume((_, value) => resolve(value));

    try {
      while (consumer.status === "active" || consumer.status === "drain") {
        yield new Promise<VALUE>((r) => {
          resolve = r;
          consumer.next();
        });
      }
    } catch (e) {
      consumer.terminate("abort");
    } finally {
      consumer.terminate("complete");
    }
  }
  abstract consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
  pipe<OUTPUT extends AnySource>(transform: ($input: this) => OUTPUT): OUTPUT {
    return transform(this);
  }
  static from<VALUE>($consumable: Consumable<VALUE>): Source<VALUE> {
    return new SourceProxy($consumable);
  }
}
type Transform<INPUT extends AnyConsumable, OUTPUT extends AnySource> = ($input: INPUT) => OUTPUT;
class SourceProxy<VALUE> extends Source<VALUE> {
  constructor(private $consumable: Consumable<VALUE>) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$consumable.consume(handler, options);
  }
}
