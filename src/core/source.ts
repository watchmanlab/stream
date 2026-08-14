import { Consumer } from "./consumer";
import { Producer } from "./producer";
import { AnySource, Consumable } from "./types";

export abstract class Source<VALUE> implements Consumable<VALUE> {
  protected _producer?: Producer<VALUE>;
  get producer() {
    return (this._producer ??= new Producer({ source: this, lastConsumerLeft: () => (this._producer = undefined) }));
  }
  abstract consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE>;
  pipe<OUTPUT extends AnySource>(transform: (input: this) => OUTPUT): OUTPUT {
    return transform(this);
  }
}
