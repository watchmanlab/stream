import { Consumable } from "./consumable";
import { Consumer } from "./consumer";
import { ExtractValue } from "./types";

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
  pipe<OUTPUT extends Consumable.AnyConsumableLike>(
    transform: ($input: this) => OUTPUT,
  ): OUTPUT extends (...args: any) => any ? Source<ExtractValue<ReturnType<OUTPUT>>> : OUTPUT {
    const output = transform(this);

    if (output instanceof Source) {
      return output as never;
    } else {
      return Source.from(output) as never;
    }
  }
  static from<VALUE>(consumableLike: Consumable.ConsumableLike<VALUE>): Source<VALUE> {
    return new SourceProxy(consumableLike);
  }
}
export namespace Source {
  export type AnySource = Source<any>;
}

class SourceProxy<VALUE> extends Source<VALUE> {
  private _consume: Consumable.Consume<VALUE>;
  constructor(consumableLike: Consumable.ConsumableLike<VALUE>) {
    super();
    this._consume = typeof consumableLike === "function" ? consumableLike : consumableLike.consume;
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this._consume(handler, options);
  }
}
