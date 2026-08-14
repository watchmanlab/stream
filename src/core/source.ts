import { Consumer } from "./consumer";
import { Producer } from "./stream";
import { AnySource, Consumable } from "./types";

export abstract class Source<VALUE> implements Consumable<VALUE> {
  private _producer?: Producer<VALUE>;
  abstract consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE>;
  pipe<OUTPUT extends AnySource>(transform: (input: this) => OUTPUT): OUTPUT {
    return transform(this);
  }
  get producer() {
    return (this._producer ??= new Producer({
      source: this,
      lastConsumerLeft: (stream, consumer) => {
        this._producer?.terminate("complete");
        this._producer = undefined;
      },
    }));
  }
}
