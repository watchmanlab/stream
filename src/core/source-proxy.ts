import { Consumer } from "./consumer";
import { Source } from "./source";
import { Producer } from "./stream";

export class SourceProxy<VALUE> extends Source<VALUE> {
  protected override _producer: Producer<VALUE>;
  constructor(_producer: Producer<VALUE>) {
    super();
    this._producer = _producer;
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this._producer.consume(handler, options);
  }
}
