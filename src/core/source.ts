import { fromIterable } from "../sources/iterable-source";
import { Consumable } from "./consumable";
import { Consumer } from "./consumer";

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
  pipe<OUTPUT>(transform: ($input: this) => OUTPUT): OUTPUT {
    return transform(this);
  }
  static from<VALUE>($consumable: Consumable<VALUE>): Source<VALUE> {
    return new SourceProxy($consumable);
  }
}

export namespace Source {
  export type AnySource = Source<any>;
}

class SourceProxy<VALUE> extends Source<VALUE> {
  constructor(private $consumable: Consumable<VALUE>) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$consumable.consume(handler, options);
  }
}
