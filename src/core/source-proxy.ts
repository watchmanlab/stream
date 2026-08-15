import { Consumer } from "./consumer";
import { Source } from "./source";
import { Stream } from "./stream";

export class SourceProxy<VALUE> extends Source<VALUE> {
  constructor(private producer: Stream<VALUE>) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.producer.consume(handler, options);
  }
}
