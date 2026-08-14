import { Consumer } from "./consumer";
import { Stream } from "./stream";
import { AnySource, Consumable } from "./types";

export abstract class Source<VALUE> implements Consumable<VALUE> {
  abstract consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE>;
  pipe<OUTPUT extends AnySource>(transform: (input: this) => OUTPUT): OUTPUT {
    return transform(this);
  }
}
