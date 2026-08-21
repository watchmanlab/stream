import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason } from "../core/types";

export class GCTokenSource extends Source<void> {
  private ref: WeakRef<object>;
  private registry?: FinalizationRegistry<unknown>;
  constructor(token: object) {
    super();
    this.ref = new WeakRef(token);
  }
  consume(options?: Consumer.Options<void> | undefined): Consumer<void> {
    return new Consumer(new ConsumerOptions(this, options));
  }
}

export function fromGCToken(token: object): GCTokenSource {
  return new GCTokenSource(token);
}

class ConsumerOptions extends Consumer.DefaultOptions<void> {
  constructor(
    private gcTokenSource: GCTokenSource,
    options?: Consumer.Options<void>,
  ) {
    super(options);
  }
  override next(consumer: Consumer<void>): void {
    super.next(consumer);
    const obj = this.gcTokenSource["ref"].deref();
    if (!obj) {
      consumer.push();
      consumer.terminate("complete");
    } else {
      this.gcTokenSource["registry"] = new FinalizationRegistry(() => {
        consumer.push();
        consumer.terminate("complete");
      });

      this.gcTokenSource["registry"].register(obj, undefined, this.gcTokenSource);
    }
  }
  override terminate(consumer: Consumer<void>, reason: TerminateReason): void {
    super.terminate(consumer, reason);
    this.gcTokenSource["registry"]?.unregister(this.gcTokenSource);
  }
}
