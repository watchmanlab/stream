import { Consumer } from "./consumer";
import { Producer } from "./producer";
import { AnySource, Consumable } from "./types";

export abstract class Source<VALUE> implements Consumable<VALUE> {
  protected _producer?: Producer<VALUE>;
  abstract consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
  pipe<OUTPUT extends AnySource>(transform: (input: this) => OUTPUT): OUTPUT {
    return transform(this);
  }
  asProducer(options?: Producer.Options<VALUE>) {
    return (this._producer ??= new Producer({
      ...options,
      source: this,
      lastConsumerLeft: () => (this._producer = undefined),
    }));
  }
}
