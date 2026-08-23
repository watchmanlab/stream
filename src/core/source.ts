import { Consumable } from "./consumable";
import { Consumer } from "./consumer";

class ThisConsumer<T> extends Consumer<T> {
  resolve?: (value: T) => void;
  protected override handler(consumer: Consumer<T>, value: T): void {
    this.resolve?.(value);
  }
}
export abstract class Source<T> implements Consumable<T>, AsyncIterable<T> {
  async *[Symbol.asyncIterator]() {
    const consumer = this.consume(new ThisConsumer<T>());
    consumer.resolve;

    try {
      while (Consumer.getState(consumer) === "active" || Consumer.getState(consumer) === "drain") {
        yield new Promise<T>((r) => {
          consumer.resolve = r;

          Consumer.next(consumer);
        });
      }
    } catch (e) {
      Consumer.terminate(consumer, "abort");
    } finally {
      Consumer.terminate(consumer, "complete");
    }
  }
  abstract consume<C extends Consumer<T>>(consumer: C): C;
  pipe<OUTPUT>(transform: ($input: this) => OUTPUT): OUTPUT {
    return transform(this);
  }
  static from<T>($consumable: Consumable<T>): Source<T> {
    return new SourceProxy($consumable);
  }
}
export namespace Source {
  export type AnySource = Source<any>;
}

class SourceProxy<T> extends Source<T> {
  constructor(private $consumable: Consumable<T>) {
    super();
  }
  override consume<C extends Consumer<T>>(consumer: C): C {
    return this.$consumable.consume(consumer);
  }
}
